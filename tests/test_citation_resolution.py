"""Normalization is not existence; target selection and source text govern lookup."""
import duckdb

from spicy_regs.citation_resolution import resolve_citations


def occurrence(kind="public_law", key="114-public-254", **extra):
    return {"document_kind": "budget_volume", "document_key": "BUDGET-2027", "text_sha256": "held",
            "cite_kind": kind, "target_key": key, "target_resolved": "true", "span_start": "10",
            "matched_text": "Public Law 114–254", "target_rule": "public_law", "rule_version": "003", **extra}


DIGESTS = {("budget_volume", "BUDGET-2027"): "held"}
PINS = {"laws": {"artifact_digest": "sha256:one"}}


def resolve(con, items, pins=PINS, **kwargs):
    return resolve_citations(con, items, pins, source_digests=DIGESTS, **kwargs)


def laws():
    con = duckdb.connect()
    con.execute("CREATE TABLE laws(law_id VARCHAR, congress VARCHAR, law_type VARCHAR, number VARCHAR)")
    con.execute("INSERT INTO laws VALUES ('114-public-254','114','public','254')")
    return con


def test_normalized_missing_and_new_target_without_reextracting_source():
    with laws() as con:
        item = occurrence(key="119-public-999")
        missing = resolve(con, [item])["occurrences"][0]
        assert missing["target_resolved"] == "true" and missing["target_status"] == "missing"
        con.execute("INSERT INTO laws VALUES ('119-public-999','119','public','999')")
        found = resolve(con, [item], {"laws": {"artifact_digest": "sha256:two"}})["occurrences"][0]
        assert found["target_status"] == "found"
        assert found["occurrence_key"] == missing["occurrence_key"]
        assert found["target_snapshot"] != missing["target_snapshot"]
        assert found["matched_text"] == item["matched_text"]


def test_ambiguous_unsupported_unread_and_partial_keys_are_not_missing():
    with laws() as con:
        con.execute("INSERT INTO laws VALUES ('114-public-254','115','public','254')")
        items = [occurrence(), occurrence("case_docket_number", "20-1"), occurrence("bill_number", "119-hr-1"),
                 occurrence(target_resolved="false"), occurrence(text_sha256="old"), occurrence(text_sha256=None)]
        rows = resolve(con, items, {**PINS, "congress_bills": {"artifact_digest": "sha256:b"}})["occurrences"]
        assert [r["target_status"] for r in rows] == ["ambiguous","unsupported","not_checked","not_checked","not_checked","not_checked"]
        assert len(rows[0]["candidate_keys"]) == 2
        assert rows[2]["reason"] == "target_read_failure"
        assert rows[3]["reason"] == "unsettled_key"
        assert rows[4]["source_status"] == "stale_source"
        assert rows[5]["source_status"] == "unread_source"


def test_unique_target_keys_are_batched_and_occurrences_are_preserved():
    with laws() as con:
        result = resolve(con, [occurrence(span_start=str(i)) for i in range(4)])
        assert len(result["occurrences"]) == 4
        assert len({r["occurrence_key"] for r in result["occurrences"]}) == 4
        assert result["coverage"]["distinct_target_keys_read"] == 1
        assert result["coverage"]["target_batches"] == 1
        limited = resolve(con, [occurrence(), occurrence(key="1-public-1")], max_target_keys=1)
        assert limited["coverage"]["partial"]
        assert limited["occurrences"][1]["reason"] == "target_key_limit"


def test_fr_dates_alternate_system_and_set_valued_rin():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE federal_register(document_number VARCHAR, publication_date VARCHAR, volume VARCHAR, start_page VARCHAR)")
        con.execute("INSERT INTO federal_register VALUES ('2026-1','2026-01-01','91','1'),('2026-1','2026-02-01','91','20')")
        con.execute("CREATE TABLE unified_agenda(rin VARCHAR, agenda_edition VARCHAR)")
        con.execute("INSERT INTO unified_agenda VALUES ('1234-AB12','202410'),('1234-AB12','202504')")
        pins = {name: {"artifact_digest": name} for name in ["federal_register", "unified_agenda"]}
        items = [occurrence("federal_register_cite","91-20"),occurrence("rin","1234-AB12")]
        rows = resolve(con,items,pins)["occurrences"]
        assert [r["target_status"] for r in rows] == ["found","found"]
        assert rows[0]["candidate_keys"] == [{"document_number":"2026-1","publication_date":"2026-02-01"}]
        assert rows[1]["expected_cardinality"] == "many" and len(rows[1]["candidate_keys"]) == 2
        limited = resolve(con,[items[1]],pins,max_candidates=1)["occurrences"][0]
        assert limited["target_status"] == "not_checked" and limited["reason"] == "candidate_limit"


def test_unpinned_selection_and_source_digest_are_required_for_text():
    with laws() as con:
        rows = resolve(con,[occurrence()],{"laws":{"status":"legacy_unversioned"}})["occurrences"]
        assert rows[0]["reason"] == "target_snapshot_unavailable"
        assert resolve_citations(con,[occurrence()],PINS)["occurrences"][0]["source_status"] == "unread_source"
        native = {"cite_kind":"public_law","target_key":"114-public-254","document_key":"native"}
        assert resolve_citations(con,[native],PINS)["occurrences"][0]["target_status"] == "found"


def test_target_timeout_is_explicit_and_preserves_other_results():
    class TimedOut:
        def execute(self, *args):
            raise TimeoutError("details must not leak")
    row = resolve(TimedOut(),[occurrence()])["occurrences"][0]
    assert row["target_status"] == "not_checked" and row["reason"] == "target_timeout"
    assert "details" not in str(row)


def test_timeout_does_not_start_another_batch_after_request_timer_expires():
    class TimedOut:
        calls = 0

        def execute(self, *args):
            self.calls += 1
            raise TimeoutError("request timer expired")
    con = TimedOut()
    result = resolve(con, [occurrence(), occurrence("bill_number", "119-hr-1")],
                     {**PINS, "congress_bills": {"artifact_digest": "sha256:b"}})
    assert con.calls == 1
    assert all(row["reason"] == "target_timeout" for row in result["occurrences"])
    assert result["coverage"]["distinct_target_keys_read"] == 0


def test_gao_and_crs_keys_are_looked_up_as_spicy_docs_prints_them():
    """GAO resolves on the printed number, else the slug; a key is never case-folded (DRY scout S3).

    spicy-docs upper-cases GAO and CRS ids as it reads them, so 0 of 342 held keys are lowercase; a printed
    number whose product slug differs (``d23105520``) is found by the number the citation prints.
    """
    with duckdb.connect() as con:
        con.execute("CREATE TABLE gao_reports(report_id VARCHAR, report_number VARCHAR)")
        con.execute("INSERT INTO gao_reports VALUES ('d23105520','GAO-23-105520'),('gao-17-317',NULL)")
        con.execute("CREATE TABLE crs_reports(report_id VARCHAR, update_date TIMESTAMPTZ)")
        con.execute("INSERT INTO crs_reports VALUES ('R47101','2026-07-17T00:00:00Z'),('R47101','2026-07-18T00:00:00Z')")
        pins = {name: {"artifact_digest": name} for name in ("gao_reports", "crs_reports")}
        items = [occurrence("gao_product_id", "GAO-23-105520"), occurrence("gao_product_id", "GAO-17-317"),
                 occurrence("crs_report_id", "R47101"), occurrence("crs_report_id", "r47101")]
        rows = resolve(con, items, pins)["occurrences"]
    assert [(r["target_key"], r["target_status"]) for r in rows] == [
        ("GAO-23-105520", "found"), ("GAO-17-317", "found"), ("R47101", "found"), ("r47101", "missing")]
    assert rows[0]["candidate_keys"] == [{"report_id": "d23105520"}]
    assert rows[2]["match_count"] == 1  # Revisions of one report are not ambiguous report identities.


# Real granules of the 2025 and 2026 editions: 2 CFR part 200 is held once, 10 CFR part 830 in both editions.
CFR_ROWS = [
    ("CFR-2025-title2-vol1-part200", "CFR-2025-title2-vol1", "2-200", None, "true"),
    ("CFR-2025-title2-vol1-part200-subpartA", "CFR-2025-title2-vol1", "2-200", None, "false"),
    ("CFR-2025-title2-vol1-part200-appI", "CFR-2025-title2-vol1", "2-200", None, "false"),
    ("CFR-2025-title2-vol1-part200-toc-id312", "CFR-2025-title2-vol1", "2-200", None, "false"),
    ("CFR-2025-title2-vol1-sec200-1", "CFR-2025-title2-vol1", "2-200.1", "1", "false"),
    ("CFR-2025-title10-vol5-part830", "CFR-2025-title10-vol5", "10-830", None, "true"),
    ("CFR-2025-title10-vol5-part830-subpartA", "CFR-2025-title10-vol5", "10-830", None, "false"),
    ("CFR-2026-title10-vol5-part830", "CFR-2026-title10-vol5", "10-830", None, "true"),
    ("CFR-2026-title10-vol5-part830-subpartA", "CFR-2026-title10-vol5", "10-830", None, "false"),
]


def cfr_sections(part_granule=True):
    con = duckdb.connect()
    con.execute("CREATE TABLE cfr_sections(granule_id VARCHAR, package_id VARCHAR, cfr_ref VARCHAR, section VARCHAR"
                + (", part_granule VARCHAR)" if part_granule else ")"))
    con.executemany(f"INSERT INTO cfr_sections VALUES ({', '.join('?' * (5 if part_granule else 4))})",
                    [row if part_granule else row[:4] for row in CFR_ROWS])
    return con


CFR_PINS = {"cfr_sections": {"artifact_digest": "sha256:cfr"}}


def test_a_part_citation_finds_the_part_granule_once_per_held_edition():
    with cfr_sections() as con:
        items = [occurrence("cfr_section", key) for key in ("2-200", "10-830", "2-200.1")]
        part, editions, section = resolve(con, items, CFR_PINS)["occurrences"]
        assert part["target_status"] == "found" and part["match_count"] == 1
        assert part["candidate_keys"] == [{"package_id": "CFR-2025-title2-vol1",
                                           "granule_id": "CFR-2025-title2-vol1-part200"}]
        assert editions["target_status"] == "ambiguous"
        assert [c["package_id"] for c in editions["candidate_keys"]] == ["CFR-2025-title10-vol5", "CFR-2026-title10-vol5"]
        assert section["target_status"] == "found"
        assert section["candidate_keys"][0]["granule_id"] == "CFR-2025-title2-vol1-sec200-1"


def test_a_cfr_generation_without_part_granule_reads_every_keyed_row():
    with cfr_sections(part_granule=False) as con:
        part = resolve(con, [occurrence("cfr_section", "2-200")], CFR_PINS)["occurrences"][0]
        assert part["target_status"] == "ambiguous" and part["match_count"] == 4 and part["reason"] is None


# M8 (round 6): a Statutes pinpoint inside a law's page range. Ranges are the 119th's PLAW files' own page markers
# (GovInfo PLAW-119-public.zip pinned 2026-09-14, read by spicy-docs' shape_law; impl-W/m8/ranges.out).

def statutes(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE laws(congress VARCHAR, law_type VARCHAR, number VARCHAR, statutes_at_large_volume VARCHAR,"
                " statutes_at_large_page VARCHAR, statutes_at_large_last_page VARCHAR)")
    con.executemany("INSERT INTO laws VALUES (?, ?, ?, ?, ?, ?)", rows)
    return con


PLAW_119 = [("119", "public", "21", "139", "72", "401"), ("119", "public", "74", "140", "5", "172"),
            ("119", "public", "75", "140", "173", "739"), ("119", "public", "1", "139", "3", None)]


def _pinpoints(con, *keys):
    rows = resolve(con, [occurrence("statutes_at_large", key, span_start=str(i)) for i, key in enumerate(keys)])
    return [(r["target_status"], [c["number"] for c in r["candidate_keys"]], r.get("match_basis"))
            for r in rows["occurrences"]]


def test_a_page_inside_a_held_laws_range_resolves_to_that_law_by_range():
    """140 Stat. 74 is inside Pub. L. 119-74 (140 Stat. 5-172): found, and the reply says the match is by range."""
    with statutes(PLAW_119) as con:
        assert _pinpoints(con, "140-74", "140-5", "139-401", "140-740") == [
            ("found", ["74"], "page_range"), ("found", ["74"], "first_page"), ("found", ["21"], "page_range"),
            ("missing", [], None)]


def test_a_law_not_yet_read_for_its_last_page_matches_its_first_page_only():
    with statutes(PLAW_119) as con:
        assert _pinpoints(con, "139-3", "139-4") == [("found", ["1"], "first_page"), ("missing", [], None)]


def test_a_page_two_laws_share_names_both_and_picks_neither():
    """One law ending and the next starting on one page (none of the 119th's do; the shape is the publisher's)."""
    shared = [("119", "public", "74", "140", "5", "172"), ("119", "public", "75", "140", "172", "739")]
    with statutes(shared) as con:
        assert _pinpoints(con, "140-172") == [("ambiguous", ["74", "75"], "first_page_and_page_range")]


def test_a_laws_generation_before_the_last_page_column_matches_first_pages_as_before():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE laws AS SELECT * EXCLUDE (statutes_at_large_last_page) FROM (VALUES "
                    "('119', 'public', '74', '140', '5', '172')) t(congress, law_type, number, statutes_at_large_volume,"
                    " statutes_at_large_page, statutes_at_large_last_page)")
        assert _pinpoints(con, "140-5", "140-74") == [("found", ["74"], "first_page"), ("missing", [], None)]
