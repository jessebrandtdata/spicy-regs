"""Metadata failures cannot hide data or invent relationships or source claims."""

from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path

import pytest

from spicy_regs.explorer_metadata import build_bundle, public_url, same_metadata


def publisher_script():
    path = Path(__file__).parents[1] / "scripts/publish_explorer_metadata.py"
    spec = importlib.util.spec_from_file_location("publish_explorer_metadata", path)
    assert spec is not None and spec.loader is not None
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    return script


def descriptor(columns):
    return {"columns": [[name, "VARCHAR"] for name in columns], "rows": 1, "byteSize": 100}


def index():
    return {"format": "spicy-regs-publication", "version": 2, "families": {
        "example": {"artifactDigest": "sha256:" + "a" * 64,
                    "tables": {"child.parquet": descriptor(["parent_id", "edition"]),
                               "parent.parquet": descriptor(["id", "edition"]),
                               "brand_new.parquet": descriptor(["id"])}}}}


def join():
    return {"child": "child", "child_columns": ["parent_id", "edition"], "parent": "parent",
            "parent_columns": ["id", "edition"], "kind": "complete", "expected_cardinality": "one"}


def args():
    return {"descriptions": {"child": {"label": "Child", "columns": []},
                             "parent": {"label": "Parent", "columns": []}},
            "registry": {"sources": {"original": {"id": "original", "name": "Publisher",
                                                   "url": "https://example.org/", "kind": "government"}},
                         "families": {}, "tables": {"child": {"sources": ["original"], "inputs": ["parent"]}}},
            "join_record": {"joins": [join()]}, "audit": {},
            "generated_at": "2026-10-03T00:00:00Z"}


def test_financial_profile_source_identity_and_current_schema():
    from spicy_regs.contract_types import described_schema
    from spicy_regs.transforms.build_fec_financial_profiles import OUTPUT, SCHEMA, TABLE

    columns = [list(pair) for pair in described_schema(SCHEMA)]
    digest = "sha256:" + "a" * 64
    selected = {"format": "spicy-regs-publication", "version": 2, "families": {
        "fec-financial-profiles": {"artifactDigest": digest,
                                   "tables": {OUTPUT: {"columns": columns, "rows": 1, "byteSize": 100}}}}}
    bundle = build_bundle(selected, generated_at="2026-10-20T00:00:00Z")
    table = bundle["tables"][TABLE]
    assert table["metadataStatus"] == table["sourceStatus"] == "documented"
    assert table["sources"][0]["id"] == "fec"
    assert table["inputs"] == []  # mapped from the captured CSVs, not from a fec-query table
    assert table["recordIdentity"] == {
        "columns": ["profile_id"], "basis": "declared_main_key", "uniqueness": "unknown"}
    assert table["family"] == "fec-financial-profiles" and table["publicationSchema"] == columns
    assert bundle["publication"]["families"][table["family"]] == digest
    assert table["modelGenerated"] is False


def test_selected_government_sources_preserve_selected_tables():
    from spicy_regs.contract_types import described_schema
    from spicy_regs.explorer_metadata import read_json
    from spicy_regs.transforms.build_census_acs import SCHEMA as acs_schema
    from spicy_regs.transforms.build_executive_orders import SCHEMA as eo_schema
    from spicy_regs.transforms.build_grants_opportunities import SCHEMA as grants_schema
    from spicy_regs.transforms.build_treasury_fiscal import SCHEMAS as treasury_schemas

    scopes = [
        ("census-acs", {"census_acs_county_observations": acs_schema},
         "census-acs", "https://www.census.gov/programs-surveys/acs"),
        ("treasury-fiscal", treasury_schemas, "treasury-fiscal", "https://fiscaldata.treasury.gov/"),
        ("grants-opportunities", {"grants_opportunity_observations": grants_schema},
         "grants", "https://www.grants.gov/"),
        ("executive-orders", {"executive_order_observations": eo_schema},
         "register", "https://www.federalregister.gov/"),
    ]
    selected = {"format": "spicy-regs-publication", "version": 2, "families": {}}
    for number, (family, schemas, _, _) in enumerate(scopes, 1):
        selected["families"][family] = {
            "artifactDigest": "sha256:" + str(number) * 64,
            "tables": {name + ".parquet": {
                "columns": [list(pair) for pair in described_schema(schema)],
                "rows": 1, "byteSize": 100,
            } for name, schema in schemas.items()},
        }
    registry = read_json("explorer_sources.json")
    previous_registry = deepcopy(registry)
    for family, _, _, _ in scopes:
        previous_registry["families"].pop(family, None)
    previous = build_bundle(selected, registry=previous_registry)
    current = build_bundle(selected)
    assert current["publication"] == previous["publication"]
    for family, schemas, source, url in scopes:
        assert current["publication"]["families"][family] == selected["families"][family]["artifactDigest"]
        for name in schemas:
            table = current["tables"][name]
            assert table["metadataStatus"] == table["sourceStatus"] == "documented"
            assert [(entry["id"], entry["url"]) for entry in table["sources"]] == [(source, url)]
            assert table["family"] == family
            assert table["publicationSchema"] == selected["families"][family]["tables"][name + ".parquet"]["columns"]
            assert {key: value for key, value in table.items() if key not in {"sources", "sourceStatus"}} == {
                key: value for key, value in previous["tables"][name].items()
                if key not in {"sources", "sourceStatus"}
            }


@pytest.fixture
def measurement_refresh(tmp_path, monkeypatch):
    """Run the publisher with real construction/verifiers and tiny measured main files."""
    from botocore.exceptions import ClientError
    from spicy_regs import explorer_metadata
    from spicy_regs.sources import cloudflare, r2
    from spicy_regs.navigation_measurements import MeasurementCache
    from tests.test_navigation_measurements import selection

    script = publisher_script()
    selected, paths, spec = selection(tmp_path)
    proof = MeasurementCache(tmp_path / "cache").measure(selected, paths, spec, source_identity=("id",))
    receipt = {"results": [proof]}
    original_receipt = script.canonical_bytes(receipt)
    reads, writes, purges = [], [], []

    class Store:
        objects = {script.INDEX_V2_KEY: script.canonical_bytes(selected),
                   script.MEASUREMENTS_KEY: original_receipt}
        failure = None

        def get_object(self, **kwargs):
            name = kwargs["Key"]
            reads.append(name)
            if name == script.MEASUREMENTS_KEY and self.failure:
                raise self.failure
            if name not in self.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(self.objects[name])}

        def put_object(self, **kwargs):
            writes.append(kwargs["Key"])
            self.objects[kwargs["Key"]] = kwargs["Body"]

    store = Store()
    monkeypatch.setenv("R2_BUCKET_NAME", "test")
    monkeypatch.setattr(r2, "require_credentials", lambda *_: None)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: purges.extend(urls))
    monkeypatch.setattr(script, "get_public", lambda *_: pytest.fail("Publishing must use authoritative control reads"))
    monkeypatch.setattr(script, "other_tables", lambda *a, **kw: {})
    monkeypatch.setattr(script.subprocess, "check_output", lambda *a, **kw: "fixture-revision\n")
    monkeypatch.setattr(explorer_metadata, "publication_descriptions", lambda _, **kwargs: {
        "source": {"label": "Source", "columns": [], "identity_columns": ["id"]},
        "target": {"label": "Target", "columns": []}})
    maintained_read = explorer_metadata.read_json
    monkeypatch.setattr(explorer_metadata, "read_json", lambda name:
                        {"joins": [], "navigation": [spec]} if name == "table_joins.json"
                        else maintained_read(name))
    output = tmp_path / "output" / script.KEY
    argv = ["publish_explorer_metadata.py", "--publish", "--base-url", "https://data.example.org",
            "--output", str(output)]
    monkeypatch.setattr("sys.argv", argv)
    return script, store, selected, spec, output, argv, reads, writes, purges, original_receipt


def test_default_companion_survives_the_next_refresh_without_proof_changes(measurement_refresh):
    script, store, _, _, output, _, reads, writes, purges, original = measurement_refresh
    script.main()
    first = json.loads(output.read_bytes())
    directions = first["navigation"][0]["targets"][0]["directions"]
    assert directions["forward"]["measurement"]["status"] == "measured"
    assert directions["reverse"]["measurement"]["status"] == "measured"
    assert [directions["forward"]["measurement"][name] for name in
            ("eligible", "matched", "missing", "ambiguous")] == [4, 3, 1, 1]
    script.main()
    assert json.loads(output.read_bytes())["navigation"] == first["navigation"]
    assert reads.count(script.MEASUREMENTS_KEY) == 2
    assert writes == [script.KEY]
    assert purges == ["https://data.example.org/" + script.KEY]
    assert store.objects[script.MEASUREMENTS_KEY] == original


@pytest.mark.parametrize("changed", ["input", "recipe", "absent"])
def test_default_companion_refuses_stale_bindings_or_remains_optional(measurement_refresh, changed):
    script, store, selected, spec, output, _, _, writes, _, original = measurement_refresh
    if changed == "input":
        selected["families"]["fixture"]["tables"]["source.parquet"]["sha256"] = "sha256:" + "c" * 64
        store.objects[script.INDEX_V2_KEY] = script.canonical_bytes(selected)
    elif changed == "recipe":
        spec["ruleVersion"] = "changed-recipe/2"
    else:
        del store.objects[script.MEASUREMENTS_KEY]
    script.main()
    directions = json.loads(output.read_bytes())["navigation"][0]["targets"][0]["directions"]
    assert directions["forward"]["measurement"]["status"] == "unknown"
    assert directions["reverse"]["measurement"]["status"] == "unknown"
    assert writes == [script.KEY]
    if changed != "absent":
        assert store.objects[script.MEASUREMENTS_KEY] == original


@pytest.mark.parametrize("bad", [b"broken", b"\xff", b"null", b"[]", b"{}", b'{"results":null}',
                                  b'{"results":{}}', b'{"results":[null]}'])
def test_invalid_companion_stops_before_output_or_publication(measurement_refresh, bad):
    script, store, _, _, output, _, _, writes, purges, _ = measurement_refresh
    old = b'{"existing":"metadata"}'
    store.objects[script.KEY] = old
    store.objects[script.MEASUREMENTS_KEY] = bad
    with pytest.raises(ValueError, match="Navigation measurement receipt"):
        script.main()
    assert not output.exists()
    assert writes == purges == []
    assert store.objects[script.KEY] == old


def test_oversized_companion_is_bounded_before_output_or_publication(measurement_refresh):
    script, store, _, _, output, _, _, writes, purges, _ = measurement_refresh
    store.objects[script.MEASUREMENTS_KEY] = b" " * (script.METADATA_LIMIT + 1)
    with pytest.raises(ValueError, match="exceeds"):
        script.main()
    assert not output.exists()
    assert writes == purges == []


@pytest.mark.parametrize("failure", ["AccessDenied", "transport"])
def test_companion_storage_failures_never_erase_counts(measurement_refresh, failure):
    from botocore.exceptions import ClientError
    script, store, _, _, output, _, _, writes, purges, _ = measurement_refresh
    store.failure = (ClientError({"Error": {"Code": failure}}, "GetObject")
                     if failure == "AccessDenied" else RuntimeError("transport unavailable"))
    with pytest.raises((ClientError, RuntimeError)):
        script.main()
    assert not output.exists()
    assert writes == purges == []


def test_explicit_receipt_overrides_unreadable_companion(measurement_refresh):
    script, store, _, _, output, argv, reads, writes, _, original = measurement_refresh
    receipt = output.parent.parent / "explicit.json"
    receipt.write_bytes(original)
    argv.extend(["--measurements", str(receipt)])
    store.failure = RuntimeError("companion must not be read")
    script.main()
    assert script.MEASUREMENTS_KEY not in reads
    assert writes == [script.KEY]
    assert json.loads(output.read_bytes())["navigation"][0]["targets"][0]["directions"]["forward"]["measurement"]["status"] == "measured"


def test_explicit_receipt_uses_the_same_bounded_parser(measurement_refresh):
    script, _, _, _, output, argv, reads, writes, _, _ = measurement_refresh
    receipt = output.parent.parent / "explicit.json"
    receipt.write_bytes(b" " * (script.METADATA_LIMIT + 1))
    argv.extend(["--measurements", str(receipt)])
    with pytest.raises(ValueError, match="receipt exceeds"):
        script.main()
    assert script.MEASUREMENTS_KEY not in reads
    assert not output.exists()
    assert writes == []


def test_measurement_parser_accepts_the_limit_and_requires_result_objects():
    script = publisher_script()
    assert script.measurement_results(None) == []
    raw = b'{"results":[]}'
    assert script.measurement_results(raw + b" " * (script.METADATA_LIMIT - len(raw))) == []
    with pytest.raises(ValueError, match="exceeds"):
        script.measurement_results(raw + b" " * script.METADATA_LIMIT)


def test_public_default_reads_optional_companion_with_metadata_limit(measurement_refresh, monkeypatch):
    script, _, selected, _, output, argv, _, _, _, original = measurement_refresh
    argv.remove("--publish")
    reads = []
    monkeypatch.setattr(script, "get_public", lambda url, limit: script.canonical_bytes(selected))
    def optional(url, limit):
        reads.append((url, limit))
        return original if url.endswith("/" + script.MEASUREMENTS_KEY) else None
    monkeypatch.setattr(script, "get_optional_public", optional)
    script.main()
    assert reads == [("https://data.example.org/" + script.KEY, script.METADATA_LIMIT),
                     ("https://data.example.org/" + script.MEASUREMENTS_KEY, script.METADATA_LIMIT)]
    assert json.loads(output.read_bytes())["navigation"][0]["targets"][0]["directions"]["forward"]["measurement"]["status"] == "measured"


@pytest.mark.parametrize("status", [404, 401, 403, 503])
def test_public_optional_only_omits_an_absent_object(monkeypatch, status):
    import httpx
    script = publisher_script()
    url = "https://data.example.org/" + script.MEASUREMENTS_KEY
    failure = httpx.HTTPStatusError("read refused", request=httpx.Request("GET", url),
                                  response=httpx.Response(status))
    def fail(*_):
        raise failure
    monkeypatch.setattr(script, "get_public", fail)
    if status == 404:
        assert script.get_optional_public(url, script.METADATA_LIMIT) is None
    else:
        with pytest.raises(httpx.HTTPStatusError):
            script.get_optional_public(url, script.METADATA_LIMIT)


def test_new_table_remains_discoverable_but_never_borrowed_description_or_join():
    result = build_bundle(index(), **args())
    assert set(result["tables"]) == {"child", "parent", "brand_new"}
    assert result["tables"]["brand_new"]["metadataStatus"] == "unknown"
    assert result["tables"]["brand_new"]["sourceStatus"] == "unknown"
    assert len(result["joins"]) == 1
    assert result["tables"]["child"]["inputs"] == ["parent"]
    assert result["tables"]["parent"]["metadataStatus"] == "documented"
    assert result["tables"]["parent"]["sourceStatus"] == "unknown"


@pytest.mark.parametrize("identity,missing", [
    (["id", "native_edition"], ["native_edition"]),
    (["native_id", "edition"], ["native_id"]),
    (["native_id", "native_edition"], ["native_edition", "native_id"]),
    (["native_id"], ["native_id"]),
    (["id", "edition"], []),
    (["id"], []),
])
def test_selected_schema_exposes_a_complete_identity_or_reports_it_unavailable(identity, missing):
    options = args()
    options["descriptions"]["parent"]["identity_columns"] = identity
    before = deepcopy(options)
    parent = build_bundle(index(), **options)["tables"]["parent"]
    if missing:
        assert "identity_columns" not in parent
        assert parent["unavailableIdentity"] == {
            "columns": identity,
            "missing_columns": missing,
            "reason": "Selected published schema does not expose the complete declared identity.",
        }
    else:
        assert parent["identity_columns"] == identity
        assert "unavailableIdentity" not in parent
    assert parent["publicationSchema"] == [["id", "VARCHAR"], ["edition", "VARCHAR"]]
    assert options == before


def test_still_published_processing_tables_use_the_canonical_dictionary(monkeypatch):
    from spicy_regs import data_dictionary, explorer_metadata
    options = args()
    known = options.pop("descriptions")
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: deepcopy(known))
    def load_descriptions():
        return {"brand_new": {"label": "Saved collection log", "summary": "Records the requests made.",
                              "category": "processing_evidence",
                              "grain": "One saved request.", "identity_columns": ["id"],
                              "coverage": "Only saved requests.", "columns": {"id": "Request ID."}},
                "unpublished": {"label": "Not published"}}
    monkeypatch.setattr(data_dictionary, "load_curated_descriptions", load_descriptions)
    result = build_bundle(index(), **options)
    table = result["tables"]["brand_new"]
    assert table["metadataStatus"] == "documented"
    assert table["label"] == "Saved collection log"
    assert table["category"] == "processing_evidence"
    assert table["grain"] == "One saved request."
    assert table["recordIdentity"] == {
        "columns": ["id"], "basis": "declared_main_key", "uniqueness": "unknown"}
    assert table["columns"] == [{"column_name": "id", "column_type": "VARCHAR", "description": "Request ID."}]
    assert "unpublished" not in result["tables"]
    assert len(result["joins"]) == 1


def test_bill_collection_checkpoints_have_an_explicit_evidence_category():
    from spicy_regs.data_dictionary import load_curated_descriptions

    descriptions = load_curated_descriptions()
    for name in ("bill_family_archives", "bill_family_backfills", "bill_family_backfill_walks"):
        assert descriptions[name]["category"] == "processing_evidence"
    # Campaign-finance receipts are substantive financial observations.
    assert descriptions["fec_receipts"].get("category") != "processing_evidence"


def test_published_fec_roles_and_count_units_survive_dictionary_fallback(monkeypatch):
    from spicy_regs import explorer_metadata

    # These published outputs are absent from the installed schema metadata.
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: {})
    expected = {
        "fec_api_response_controls": ("diagnostics", "response fields"),
        "fec_agency_mapping_dispositions": ("diagnostics", "mapping results"),
        "fec_reference_filing_feed_items": ("reference_data", "feed items"),
        "fec_reference_source_pages": ("reference_data", "source pages"),
        "fec_collections": ("processing_evidence", "collections"),
        "fec_collection_selection": ("processing_evidence", "selection decisions"),
        "fec_record_evidence": ("source_evidence", "evidence links"),
        "fec_filing_header_associations": ("source_evidence", "association decisions"),
        "fec_filing_definition_evidence": ("source_evidence", "source references"),
        "fec_filing_definitions": ("reference_data", "filing layouts"),
        "fec_source_catalog": ("reference_data", "sources"),
        "fec_source_records": ("source_data", "source records"),
    }
    live = {name: {"schema": [["record_id", "VARCHAR"]]} for name in expected}
    result = explorer_metadata.publication_descriptions(live)
    for name, (category, unit) in expected.items():
        assert result[name]["category"] == category
        assert result[name]["row_unit"] == unit
        assert "record_id" in {column["column_name"] for column in result[name]["columns"]}
    assert "committee_report_reads" not in result  # Documentation cannot invent publication.


@pytest.mark.parametrize("receipt_owner, changed_schema", [(False, False), (True, False), (False, True)])
def test_earlier_fec_names_use_only_exact_receipt_free_dictionary_declarations(receipt_owner, changed_schema):
    from spicy_regs import data_dictionary

    name = "fec_research_source_pages"
    historical = json.loads((data_dictionary.REPO_ROOT / "data_dictionary/fec_historical_definitions.json").read_text())
    entry = historical["tables"][name]
    schema = deepcopy(entry["columns"])
    if changed_schema:
        schema[0][1] = "BIGINT" if schema[0][1] != "BIGINT" else "VARCHAR"
    owner: dict[str, object] = {"artifactDigest": "sha256:" + "a" * 64,
             "tables": {name + ".parquet": {"columns": schema, "rows": 1, "byteSize": 100}}}
    if receipt_owner:
        owner["etlReceipts"] = {"version": 1}
    selected = {"format": "spicy-regs-publication", "version": 2, "families": {"fec-query": owner}}
    result = build_bundle(selected, join_record={"joins": []}, audit={}, generated_at="2026-10-31T00:00:00Z")
    table = result["tables"][name]
    assert table["publicationSchema"] == schema
    if receipt_owner or changed_schema:
        assert table["metadataStatus"] == "unknown"
        assert "recordIdentity" not in table
    else:
        assert table["metadataStatus"] == "documented"
        assert table["grain"] == entry["description"]["grain"]
        assert table["summary"] == entry["description"]["summary"]
        assert table["recordIdentity"]["columns"] == entry["description"]["identity_columns"]
        assert {column["column_name"]: column["description"] for column in table["columns"]} == entry["columnDescriptions"]
        assert "fec_reference_source_pages" not in result["tables"]


def test_packaged_fec_metadata_uses_the_same_roles_as_the_fallback(monkeypatch):
    from spicy_regs import explorer_metadata

    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: {
        "fec_api_response_controls": {"label": "Known table", "category": "query_data"},
        "fec_receipts": {"category": "query_data"},
    })
    live = {name: {"schema": []} for name in ("fec_api_response_controls", "fec_receipts")}
    result = explorer_metadata.publication_descriptions(live)
    assert result["fec_api_response_controls"]["category"] == "diagnostics"
    assert result["fec_receipts"]["category"] == "query_data"


@pytest.mark.parametrize("change", [
    {"child_columns": ["missing"]}, {"child_columns": ["parent_id"]},
    {"child_columns": []}, {"child_columns": ["parent_id", "parent_id"]},
    {"parent": "unregistered"}, {"expected_cardinality": "sure"}, {"kind": "inferred"},
])
def test_rejects_invalid_keys_and_cardinality(change):
    options = args()
    options["join_record"]["joins"][0].update(change)
    with pytest.raises(ValueError):
        build_bundle(index(), **options)


def test_schema_changes_publish_descriptions_but_disable_old_connections():
    changed, options = index(), args()
    options["descriptions"]["parent"]["columns"] = [
        {"column_name": "id", "column_type": "VARCHAR"},
        {"column_name": "edition", "column_type": "VARCHAR"},
    ]
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["new_id", "VARCHAR"]]
    result = build_bundle(changed, **options)
    assert result["tables"]["parent"]["publicationSchema"] == [["new_id", "VARCHAR"]]
    assert result["joins"] == []
    assert result["omittedJoins"] == [{"child": "child", "parent": "parent",
                                       "reason": "Fields no longer published: parent.id, parent.edition."}]
    assert result["tables"]["child"]["joinAudit"]["status"] == "missing"
    assert result["tables"]["child"]["unavailableJoins"] == result["omittedJoins"]


def test_curated_fallback_preserves_removed_key_validation_without_publishing_it(monkeypatch):
    from spicy_regs import data_dictionary, explorer_metadata
    changed, options = index(), args()
    known = options.pop("descriptions")
    del known["parent"]
    monkeypatch.setattr(explorer_metadata, "read_json", lambda _: deepcopy(known))
    monkeypatch.setattr(data_dictionary, "load_curated_descriptions", lambda: {
        "parent": {"label": "Parent", "identity_columns": ["id", "edition"],
                   "columns": {"id": "Recorded ID.", "edition": "Edition."}}})
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"] = [["new_id", "VARCHAR"]]
    result = build_bundle(changed, **options)
    assert result["joins"] == []
    assert result["tables"]["parent"]["publicationSchema"] == [["new_id", "VARCHAR"]]
    assert result["tables"]["parent"]["columns"] == [
        {"column_name": "new_id", "column_type": "VARCHAR", "description": ""}]
    assert "recordIdentity" not in result["tables"]["parent"]
    assert result["tables"]["parent"]["unavailableIdentity"]["columns"] == ["id", "edition"]
    assert "parent.id, parent.edition" in result["omittedJoins"][0]["reason"]
    options["join_record"]["joins"][0]["parent_columns"][0] = "never_documented"
    with pytest.raises(ValueError, match="missing columns"):
        build_bundle(changed, **options)


def test_new_current_key_can_be_declared_without_borrowing_old_fields():
    changed, options = index(), args()
    changed["families"]["example"]["tables"]["parent.parquet"]["columns"][0][0] = "new_id"
    options["join_record"]["joins"][0]["parent_columns"][0] = "new_id"
    result = build_bundle(changed, **options)
    assert all(result["joins"][0][k] == v for k, v in options["join_record"]["joins"][0].items())
    assert result["omittedJoins"] == []


def test_unmeasured_declaration_preserves_its_kind_and_uncertainty():
    options = args()
    options["join_record"]["joins"][0].update(kind="unmeasured", reason="No measured baseline yet.")
    result = build_bundle(index(), **options)
    assert result["joins"][0]["kind"] == "unmeasured"
    assert result["joins"][0]["reason"] == "No measured baseline yet."


def test_missing_live_parent_is_explicitly_omitted_but_input_is_preserved():
    changed, options = index(), args()
    del changed["families"]["example"]["tables"]["parent.parquet"]
    options["descriptions"]["parent"]["columns"] = [
        {"column_name": "id", "column_type": "VARCHAR"}, {"column_name": "edition", "column_type": "VARCHAR"}]
    options["audit"] = {"child": {"status": "connected", "reason": "Connected when reviewed."}}
    result = build_bundle(changed, **options)
    assert result["joins"] == []
    assert result["omittedJoins"][0]["parent"] == "parent"
    assert result["tables"]["child"]["inputs"] == ["parent"]
    assert result["tables"]["child"]["joinAudit"]["status"] == "missing"
    assert "unavailable" in result["tables"]["child"]["joinAudit"]["reason"]
    assert result["tables"]["child"]["unavailableJoins"][0]["parent"] == "parent"


def test_duplicate_canonical_join_is_rejected():
    options = args()
    options["join_record"]["joins"].append(join())
    with pytest.raises(ValueError, match="Duplicate join declaration"):
        build_bundle(index(), **options)


def test_timer_does_not_refresh_freshness_but_data_or_metadata_change_does():
    before = build_bundle(index(), **args())
    after = deepcopy(before)
    after["generatedAt"] = "2026-10-04T00:00:00Z"
    assert same_metadata(before, after)
    after["publication"]["families"]["example"] = "sha256:" + "b" * 64
    assert not same_metadata(before, after)


def test_invalid_source_and_undocumented_input_refused():
    options = args()
    options["registry"]["sources"]["original"]["url"] = "javascript:alert(1)"
    with pytest.raises(ValueError, match="source attribution"):
        build_bundle(index(), **options)
    options = args()
    options["registry"]["tables"]["child"]["inputs"] = ["imagined_table"]
    with pytest.raises(ValueError, match="Unknown"):
        build_bundle(index(), **options)


def test_scorecard_publishers_come_from_current_rows_not_a_hardcoded_list():
    options, data = args(), index()
    data["families"]["scorecards"] = data["families"].pop("example")
    options["scorecard_publishers"] = [{"id": "new-publisher", "name": "New Publisher",
                                        "url": "https://new.example/scorecard", "kind": "publisher"}]
    result = build_bundle(data, **options)
    assert result["tables"]["parent"]["sources"][0]["name"] == "New Publisher"
    assert result["tables"]["parent"]["sourceStatus"] == "documented"


def test_repeated_table_is_rejected():
    data = index()
    data["families"]["duplicate"] = deepcopy(data["families"]["example"])
    with pytest.raises(ValueError, match="repeated table"):
        build_bundle(data, **args())


def test_source_registry_distinguishes_deterministic_attributes_and_model_output():
    import json
    registry = json.loads((Path(__file__).parents[1] / "src/spicy_regs/explorer_sources.json").read_text())
    assert not registry["tables"]["document_attributes"]["modelGenerated"]
    assert registry["tables"]["document_attributes"]["inputs"] == []
    assert registry["tables"]["bill_summaries"]["modelGenerated"]
    assert registry["tables"]["member_vote_terms"]["inputs"] == ["member_votes", "member_terms"]


@pytest.mark.parametrize("url", [
    "https://example.org/scorecard",
    "http://www.citizen.org/vchart00/map.htm",
    "http://www.drugpolicyaction.org/voter-guide/",
    "http://globalsolutions.org/capitol-hill/reportcard/2010",
])
def test_scorecard_reader_uses_immutable_member_path_and_checks_digest(monkeypatch, url):
    import hashlib
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources.publication import Member
    script = publisher_script()
    rows = [{"publisher_id": "new", "name": "New Publisher", "scorecard_index_url": url,
             "homepage_url": "https://example.org/", "source_url": "https://example.org/original"}]
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    raw = buffer.getvalue()
    member = Member("generations/scorecards/" + "a" * 64 + "/scorecard_publishers.parquet",
                    "sha256:" + hashlib.sha256(raw).hexdigest(), len(raw), 1)
    monkeypatch.setattr(script, "table_members", lambda *_: [member])
    seen = []
    monkeypatch.setattr(script, "get_public", lambda url, limit: seen.append(url) or raw)
    data = {"families": {"scorecards": {"tables": {"scorecard_publishers.parquet": {}}}}}
    publishers = script.scorecard_sources(data, "https://data.example.org")
    assert publishers[0]["name"] == "New Publisher"
    assert publishers[0]["url"] == url
    options, live = args(), index()
    live["families"]["scorecards"] = live["families"].pop("example")
    options["scorecard_publishers"] = publishers
    assert build_bundle(live, **options)["tables"]["parent"]["sources"][0]["url"] == url
    assert seen == ["https://data.example.org/" + member.path]
    monkeypatch.setattr(script, "get_public", lambda *_: b"wrong bytes")
    with pytest.raises(ValueError, match="differ"):
        script.scorecard_sources(data, "https://data.example.org")


@pytest.mark.parametrize("preferred, source, accepted", [
    (None, "https://example.org/original", True),
    (None, None, False),
    (None, "javascript:alert(1)", False),
    ("javascript:alert(1)", "https://example.org/original", False),
])
def test_scorecard_attribution_uses_source_only_when_preferred_links_absent(
    monkeypatch, preferred, source, accepted,
):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources.publication import Member
    script = publisher_script()
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([{
        "publisher_id": "original-only", "name": "Original Publisher",
        "scorecard_index_url": preferred, "homepage_url": None, "source_url": source,
    }]), buffer)
    raw = buffer.getvalue()
    member = Member("generations/scorecards/" + "a" * 64 + "/scorecard_publishers.parquet",
                    "sha256:" + hashlib.sha256(raw).hexdigest(), len(raw), 1)
    monkeypatch.setattr(script, "table_members", lambda *_: [member])
    monkeypatch.setattr(script, "get_public", lambda *_: raw)
    data = {"families": {"scorecards": {"tables": {"scorecard_publishers.parquet": {}}}}}
    if accepted:
        assert script.scorecard_sources(data, "https://data.example.org")[0]["url"] == source
    else:
        with pytest.raises(ValueError, match="valid public URL"):
            script.scorecard_sources(data, "https://data.example.org")


def test_historical_http_links_are_only_allowed_for_attribution():
    assert public_url("https://example.org/")
    assert not public_url("http://example.org/")
    assert public_url("http://example.org/", allow_http=True)
    for url in ["javascript:alert(1)", "file:///tmp/source", "//example.org/",
                "http://user:password@example.org/", "https://user@example.org/", "http://"]:
        assert not public_url(url, allow_http=True)


def test_metadata_data_base_still_requires_https(monkeypatch):
    script = publisher_script()
    monkeypatch.setattr("sys.argv", ["publish_explorer_metadata.py", "--base-url", "http://data.example.org"])
    monkeypatch.setattr(script, "get_public", lambda *_: pytest.fail("Insecure data base must not be read"))
    with pytest.raises(SystemExit) as error:
        script.main()
    assert error.value.code == 2


def test_failed_write_preserves_previous_object_and_only_targets_metadata():
    import io
    script = publisher_script()
    bundle = build_bundle(index(), **args())

    class Store:
        data = b'{"old":"metadata"}'
        fail = True
        def get_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            return {"Body": io.BytesIO(self.data)}
        def put_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            if self.fail:
                raise RuntimeError("R2 unavailable")
            self.data = kwargs["Body"]

    client = Store()
    before = client.data
    with pytest.raises(RuntimeError, match="unavailable"):
        script.publish_bundle(client, "test", bundle)
    assert client.data == before
    client.fail = False
    assert script.publish_bundle(client, "test", bundle) == "published"
    client.fail = True
    assert script.publish_bundle(client, "test", bundle) == "unchanged"


def test_failed_readback_never_reports_success(monkeypatch):
    script = publisher_script()
    writes = []
    class Store:
        def put_object(self, **kwargs):
            writes.append(kwargs["Key"])
    monkeypatch.setattr(script, "read_object", lambda *a, **kw: b'{}')
    with pytest.raises(ValueError, match="readback differs"):
        script.publish_bundle(Store(), "test", build_bundle(index(), **args()))
    assert writes == [script.KEY]


@pytest.mark.parametrize("previous", [b'invalid JSON', b'[]', b'null', b'{}', b'\xff'])
def test_invalid_prior_metadata_is_replaced_after_validated_build(previous):
    import io
    script = publisher_script()
    class Store:
        data = previous
        writes = 0
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(self.data)}
        def put_object(self, **kwargs):
            assert kwargs["Key"] == script.KEY
            self.data = kwargs["Body"]
            self.writes += 1
    client = Store()
    bundle = build_bundle(index(), **args())
    assert script.publish_bundle(client, "test", bundle) == "published"
    assert client.writes == 1
    assert client.data == script.canonical_bytes(bundle)


def test_declared_record_identity_and_directional_scope_do_not_infer_uniqueness():
    options = args()
    options['descriptions']['parent']['identity_columns'] = ['id', 'edition']
    options['descriptions']['child']['identity_columns'] = ['parent_id', 'edition', 'row_index']
    result = build_bundle(index(), **options)
    assert result['tables']['parent']['recordIdentity'] == {
        'columns': ['id', 'edition'], 'basis': 'declared_main_key', 'uniqueness': 'unknown'}
    assert 'recordIdentity' not in result['tables']['child']
    declared = result['joins'][0]
    assert declared['completeKey'] == {'child': False, 'parent': True}
    assert declared['directions']['forward']['measurement']['status'] == 'unknown'
    # expected_cardinality is a declared CI policy, not a current measurement.
    assert declared['expected_cardinality'] == 'one'


def test_retired_relationships_remain_accounted_without_reappearing_as_joins():
    options = args()
    options['join_record']['processing_joins'] = [join()]
    result = build_bundle(index(), **options)
    retired = result['retiredJoins'][0]
    assert retired['status'] == 'retired_processing_relationship'
    assert not retired['directions']['forward']['available']
    assert not retired['directions']['reverse']['available']
    assert len(result['joins']) == 1


def test_main_policy_identity_is_admitted_but_receipt_identity_never_is(monkeypatch):
    from spicy_regs import subject_catalog
    monkeypatch.setattr(subject_catalog, 'descriptors', lambda: {
        'parent': {'identity_fields':['id','edition'], 'receipt_only':False},
        'child': {'identity_fields':['parent_id','edition'], 'receipt_only':True}})
    result = build_bundle(index(), **args())
    assert result['tables']['parent']['recordIdentity']['columns'] == ['id','edition']
    assert 'recordIdentity' not in result['tables']['child']
    assert result['joins'][0]['completeKey'] == {'child': False, 'parent': True}


@pytest.mark.parametrize('identity', ['id', ['id', 'id'], ['id', None]])
def test_malformed_declared_record_identity_cannot_enable_detail_navigation(identity):
    options = args()
    options['descriptions']['parent']['identity_columns'] = identity
    with pytest.raises(ValueError, match='Invalid declared record identity: parent'):
        build_bundle(index(), **options)


def test_the_published_inputs_are_every_mutable_object_the_publisher_read(measurement_refresh, monkeypatch):
    """The scheduled early exit rereads exactly the bundle's inputs, so an unrecorded read would go unwatched."""
    from spicy_regs import explorer_publications
    from spicy_regs.explorer_inputs import unchanged

    script, store, _, _, output, _, reads, _, _, _ = measurement_refresh
    monkeypatch.setattr(script, "other_tables", explorer_publications.other_tables)
    script.main()
    bundle = json.loads(output.read_bytes())
    # The publisher's own output is its cache and readback, never an input.
    assert set(reads) - {script.KEY} == set(bundle["inputs"])
    asked = []
    same, _ = unchanged(store.objects[script.KEY], revision=bundle["sourceRevision"],
                        read=lambda key: asked.append(key) or store.objects.get(key))
    assert same and sorted(asked) == sorted(bundle["inputs"])
