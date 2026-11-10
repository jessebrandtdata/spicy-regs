"""Bounded Treasury fixtures exercise values, periods, revisions, Table 2 cells and existing serving."""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

import httpx

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import mcp_server
from spicy_regs.pipelines import treasury_fiscal as pipeline
from spicy_regs.source_evidence import JOURNAL
from spicy_regs.sources import publication
from spicy_regs.transforms import build_treasury_fiscal as module
from tests.generation_fakes import Store

DEBT_DATE = date(2025, 10, 31)
ISSUE = date(2025, 10, 31)
QUALIFICATION = {"sha256": "sha256:" + "a" * 64, "url": "https://fiscaldata.treasury.gov/fixture.pdf"}
FIXTURES = Path(__file__).parent / "fixtures" / "treasury"
TABLE2 = list(module.UNIT_RULES["datasets"]["mts_table_2"])


class Reader:
    def __init__(self, dataset, when, rows, *, revision="first", body=None, metadata=None):
        self.body = body or json.dumps(rows).encode()
        digest = hashlib.sha256(self.body + revision.encode()).hexdigest()
        self.pin = ArtifactPin("urn:test:treasury:" + digest, "sha256:" + digest)
        self.records = [{
            "sourceRecordId": dataset + ":" + when.isoformat() + ":" + str(index),
            "record": {
                "dataset": dataset, "selection": {"dataset": dataset, "record_date": when.isoformat(), "page_size": 50},
                "raw": raw, "field_metadata": metadata or {"labels": {"record_date": "Record Date"},
                                                           "dataTypes": {"record_date": "DATE"}, "dataFormats": {}},
                "source_locator": {"request_url": "https://api.fiscaldata.treasury.gov/fixture",
                                   "response_sha256": "sha256:" + hashlib.sha256(self.body).hexdigest(),
                                   "observed_at": "2026-10-20T00:00:00Z", "byte_size": len(self.body),
                                   "json_pointer": f"/data/{index}"},
            },
        } for index, raw in enumerate(rows)]

    def iter_records(self):
        yield from deepcopy(self.records)

    def iter_record_evidence(self):
        for item in self.records:
            yield {"sourceRecordId": item["sourceRecordId"],
                   "evidenceBlobRef": "sha256:" + hashlib.sha256(self.body).hexdigest()}

    def read_evidence(self, ref, *, max_bytes):
        assert len(self.body) <= max_bytes
        return self.body

    @classmethod
    def retained(cls, name):
        """One exact retained API response (tests/fixtures/treasury), as its release would admit it."""
        body = (FIXTURES / (name + ".json")).read_bytes()
        value = json.loads(body)
        dataset, when = name.rsplit("-", 3)[0], date.fromisoformat(name.split("-", 1)[1])
        metadata = {key: value["meta"][key] for key in ("labels", "dataTypes", "dataFormats")}
        return cls(dataset, when, value["data"], body=body, metadata=metadata)


def debt():
    return Reader("debt_to_penny", DEBT_DATE, [{
        "record_date": DEBT_DATE.isoformat(), "tot_pub_debt_out_amt": "3.03",
        "debt_held_public_amt": "2.02", "intragov_hold_amt": "1.01",
    }])


def statement(when=ISSUE, *, fiscal_years=None, receipt="1.01", revision="first"):
    """Table 1 rows: by default the complete prior fiscal year and the current year through the issue."""
    current = when.year + (when.month >= 10)
    rows = []
    for fiscal_year in fiscal_years or (current - 1, current):
        parent = {
            "record_date": when.isoformat(), "classification_id": f"header{fiscal_year}", "parent_id": "null",
            "classification_desc": f"FY {fiscal_year}", "record_fiscal_year": str(current),
            "table_nbr": "1", "data_type_cd": "S", "record_type_cd": "SL", "sequence_number_cd": "1",
            **dict.fromkeys(module.BUDGET_FIELDS.values(), "null"),
        }
        months = [month for month in (*range(10, 13), *range(1, 10))
                  if date(fiscal_year - (month >= 10), month, 1) <= when]
        children = [parent | {
            "classification_id": f"{fiscal_year}.{index}", "parent_id": parent["classification_id"],
            "classification_desc": next(name for name, number in module._MONTHS.items() if number == month),
            "data_type_cd": "D", "record_type_cd": "MTH", "sequence_number_cd": f"1.{index}",
            "current_month_gross_rcpt_amt": receipt, "current_month_gross_outly_amt": "2.02",
            "current_month_dfct_sur_amt": str(Decimal("2.02") - Decimal(receipt)),
        } for index, month in enumerate(months, 1)]
        rows += [parent, *children, parent | {
            "classification_id": f"{fiscal_year}.ytd", "parent_id": parent["classification_id"],
            "classification_desc": "Year-to-Date", "data_type_cd": "T", "sequence_number_cd": f"1.{len(months) + 1}",
            **{field: str(sum(Decimal(row[field]) for row in children)) for field in module.BUDGET_FIELDS.values()},
        }]
    return Reader("mts_table_1", when, rows, revision=revision)


#: Estimate cells ($ millions) that satisfy every Table 2 identity without a 0.00 cell.
ESTIMATES = {2: "4.00", 3: "3.00", 4: "1.00", 5: "6.00", 6: "5.50", 7: "0.50", 8: "-2.00", 9: "-2.50",
             10: "0.50", 11: "2.00", 13: "1.50", 14: "0.25", 15: "0.25"}


def table2(when=ISSUE, *, receipt="1.01", revision="first"):
    """A native 15-line Table 2 whose actuals reconcile with statement(when, receipt=receipt)."""
    current = when.year + (when.month >= 10)
    months = sum(1 for month in (*range(10, 13), *range(1, 10)) if date(current - (month >= 10), month, 1) <= when)
    outlay = Decimal("2.02")
    actual = {"current_month_budget_amt": (Decimal(receipt), outlay),
              "current_fytd_budget_amt": (Decimal(receipt) * months, outlay * months),
              "prior_fytd_budget_amt": (Decimal(receipt) * months, outlay * months)}
    rows = []
    for line, (desc, kind, parent) in module.TABLE2_LAYOUT.items():
        row = {"record_date": when.isoformat(), "classification_id": str(7000 + line),
               "parent_id": "null" if parent is None else str(7000 + parent), "classification_desc": desc,
               "table_nbr": "2", "src_line_nbr": str(line), "print_order_nbr": str(line),
               "sequence_level_nbr": "1" if parent is None else "2", "sequence_number_cd": f"{line}",
               "data_type_cd": kind, "record_type_cd": "SL", "record_fiscal_year": str(current)}
        for field in TABLE2:
            if kind == "S":
                row[field] = "null"
            elif field in actual:
                r, o = actual[field]
                zero = Decimal("0.00")
                row[field] = str({2: r, 3: r, 4: zero, 5: o, 6: o, 7: zero, 8: r - o, 9: r - o, 10: zero,
                                  11: o - r, 13: o - r, 14: zero, 15: zero}[line])
            else:
                row[field] = ESTIMATES[line]
        rows.append(row)
    return Reader("mts_table_2", when, rows, revision=revision)


def control(when=ISSUE):
    return table2(when)


def write(readers, tmp_path, issues=(ISSUE,)):
    files, _ = module.build_treasury_fiscal(
        readers, tmp_path, debt_dates=(DEBT_DATE,), report_dates=issues,
        qualifications={issue: QUALIFICATION for issue in issues},
    )
    return files


@pytest.mark.parametrize(("raw", "status"), [(None, "json_null"), ("null", "publisher_null"), ("", "blank")])
def test_literal_nulls_exact_decimals_and_raw_context(tmp_path, raw, status):
    source = statement()
    for item in source.records:
        if item["record"]["raw"]["classification_id"] in ("2026.1", "2026.ytd"):
            item["record"]["raw"]["current_month_gross_rcpt_amt"] = raw
    write([debt(), source, control()], tmp_path)
    row, = [row for row in pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()
            if row["classification_id"] == "2026.1"]
    assert row["budget_receipts_usd"] is None
    assert row["budget_outlays_usd"] == Decimal("2.02")
    assert json.loads(row["amount_statuses_json"])["budget_receipts_usd"] == status
    assert json.loads(row["raw_row_json"])["current_month_gross_rcpt_amt"] == raw
    assert json.loads(row["parent_context_json"])["record"]["raw"]["classification_desc"] == "FY 2026"
    assert row["source_release_digest"] == source.pin.artifact_digest
    assert row["qualification_report_sha256"] == QUALIFICATION["sha256"]
    assert pq.read_table(tmp_path / (module.DEBT + ".parquet")).to_pylist()[0]["total_debt_usd"] == Decimal("3.03")


def test_parent_fiscal_year_and_sequence_stay_distinct_from_report_context(tmp_path):
    issue = date(2026, 9, 30)
    write([debt(), statement(issue), control(issue)], tmp_path, (issue,))
    rows = pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()
    july, = [row for row in rows if row["classification_desc"] == "July" and row["economic_fiscal_year"] == 2025]
    assert july["sequence_number_cd_literal"] == "1.10"
    assert july["period_start"] == date(2025, 7, 1)
    assert july["report_date"] == issue
    assert json.loads(july["raw_row_json"])["record_fiscal_year"] == "2026"
    total, = [row for row in rows if row["period_kind"] == "fiscal_year_to_date" and row["economic_fiscal_year"] == 2025]
    assert total["period_start"] == date(2024, 10, 1) and total["period_end"] == date(2025, 9, 30)


@pytest.mark.parametrize("same_issue", [False, True])
def test_issue_and_same_issue_revisions_preserve_both_observations(tmp_path, same_issue):
    later = ISSUE if same_issue else date(2025, 11, 30)
    first, revised = statement(), statement(later, receipt="1.11", revision="revised")
    readers = [debt(), first, revised, control()]
    if not same_issue:
        readers.append(table2(later, receipt="1.11"))
    write(readers, tmp_path, tuple(sorted({ISSUE, later})))
    rows = pq.read_table(tmp_path / (module.STATEMENTS + ".parquet")).to_pylist()
    october = [row for row in rows if row["classification_desc"] == "October" and row["economic_fiscal_year"] == 2026]
    assert [row["budget_receipts_usd"] for row in october] == [Decimal("1.01"), Decimal("1.11")]
    assert [row["report_date"] for row in october] == [ISSUE, later]
    assert len({row["source_release_digest"] for row in october}) == 2


def test_table2_release_needs_one_reconciling_table1_release_of_its_issue(tmp_path):
    """Same-issue Table 1 revisions stay separate; Table 2 must match one of them, not all."""
    first, revised = statement(), statement(receipt="1.11", revision="revised")
    _, summary = module.build_treasury_fiscal(
        [debt(), first, revised, table2(receipt="1.11")], tmp_path / "ok", debt_dates=(DEBT_DATE,),
        report_dates=(ISSUE,), qualifications={ISSUE: QUALIFICATION})
    assert summary[0]["table1_cells_evaluated"] == 9
    with pytest.raises(ValueError, match="every Table 1 release"):
        write([debt(), first, revised, table2(receipt="1.21")], tmp_path / "refused")
    with pytest.raises(ValueError, match="carries the periods Table 2 is checked against"):
        write([debt(), statement(fiscal_years=(2026,)), table2()], tmp_path / "missing-prior-year")


def _table2_cell(reader, line, field, literal):
    reader.records[line - 1]["record"]["raw"][field] = literal


@pytest.mark.parametrize("defect", [
    "period", "parent", "date", "controls", "decimal", "deficit", "debt_total",
    "t2_on_off", "t2_receipts_outlays", "t2_financing", "t2_means", "t2_table1", "t2_layout", "t2_parent",
    "t2_repeat", "t2_unreviewed_zero", "unit_rule",
])
def test_refusal_preserves_prior_tables(tmp_path, defect):
    write([debt(), statement(), control()], tmp_path)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    source, debt_reader, cells = statement(), debt(), control()
    readers = [debt_reader, source, cells]
    if defect == "period":
        source.records.pop(1)
    elif defect == "parent":
        source.records.pop(0)
    elif defect == "date":
        source.records[-1]["record"]["raw"]["record_date"] = "2025-11-30"
    elif defect == "controls":
        readers.pop()
    elif defect == "decimal":
        source.records[1]["record"]["raw"]["current_month_gross_rcpt_amt"] = "1.001"
    elif defect == "deficit":
        source.records[1]["record"]["raw"]["current_month_dfct_sur_amt"] = "-1.01"
    elif defect == "debt_total":
        debt_reader.records[0]["record"]["raw"]["tot_pub_debt_out_amt"] = "3.00"
    elif defect == "t2_on_off":
        _table2_cell(cells, 3, "current_year_budget_est_amt", "3.10")
    elif defect == "t2_receipts_outlays":
        _table2_cell(cells, 8, "next_year_budget_est_amt", "-2.10")
    elif defect == "t2_financing":
        _table2_cell(cells, 11, "current_year_budget_est_amt", "2.10")
    elif defect == "t2_means":
        _table2_cell(cells, 15, "next_year_budget_est_amt", "0.35")
    elif defect == "t2_table1":
        _table2_cell(cells, 2, "prior_fytd_budget_amt", "1.02")
        _table2_cell(cells, 3, "prior_fytd_budget_amt", "1.02")
    elif defect == "t2_layout":
        _table2_cell(cells, 13, "classification_desc", "Borrowing")
    elif defect == "t2_parent":
        _table2_cell(cells, 3, "parent_id", "7005")
    elif defect == "t2_repeat":
        cells.records[14]["record"]["raw"]["src_line_nbr"] = "14"
    elif defect == "t2_unreviewed_zero":
        _table2_cell(cells, 14, "next_year_budget_est_amt", "0.00")
        _table2_cell(cells, 15, "next_year_budget_est_amt", "0.50")
    else:
        source.records[1]["record"]["raw"]["future_estimate_amt"] = "1.00"
    with pytest.raises(ValueError):
        write(readers, tmp_path)
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


#: Page-reviewed marker tallies and identity counts per retained issue (visual review 2026-10-31).
RETAINED_EDITIONS = {
    # 2015: a literal "null" in line 14 next_year and fractional-million estimates.
    date(2015, 3, 31): ({"value": 64, "publisher_null": 11}, {"evaluated": 39, "skipped": 1}),
    # 2025-09: line 14 estimates print dots; the prior-FYTD means of financing gap ships under its pinned exception.
    date(2025, 9, 30): ({"value": 63, "no_transactions": 2, "publisher_null": 10}, {"evaluated": 38, "skipped": 2}),
    # 2025-10: every estimate is a reviewed No Transactions cell (zero_estimates ffd92cf3).
    date(2025, 10, 31): ({"value": 39, "no_transactions": 26, "publisher_null": 10}, {"evaluated": 24, "skipped": 16}),
    date(2026, 3, 31): ({"value": 51, "no_transactions": 14, "publisher_null": 10}, {"evaluated": 28, "skipped": 12}),
    date(2026, 9, 30): ({"value": 51, "no_transactions": 14, "publisher_null": 10}, {"evaluated": 28, "skipped": 12}),
}


def test_retained_editions_type_every_cell_with_reviewed_markers(tmp_path):
    """Real retained responses: 75 cells each, page-reviewed marker tallies, identities, units and years."""
    issues = tuple(RETAINED_EDITIONS)
    readers = [Reader.retained("debt_to_penny-2026-10-16")] + [
        Reader.retained(f"mts_table_{table}-{issue.isoformat()}") for issue in issues for table in (1, 2)]
    for reader in readers:
        assert "sha256:" + hashlib.sha256(reader.body).hexdigest() in (FIXTURES / "README.md").read_text().replace(
            "| ", "| sha256:")
    _, summary = module.build_treasury_fiscal(
        readers, tmp_path, debt_dates=(date(2026, 10, 16),), report_dates=issues,
        qualifications={issue: QUALIFICATION for issue in issues})
    rows = pq.read_table(tmp_path / (module.CELLS + ".parquet")).to_pylist()
    for issue, (expected, _) in RETAINED_EDITIONS.items():
        cells = [row for row in rows if row["report_date"] == issue]
        tally = {kind: sum(row["marker_kind"] == kind for row in cells) for kind in {row["marker_kind"] for row in cells}}
        assert len(cells) == 75 and tally == expected
    assert {date.fromisoformat(entry["report_date"]): entry["identities"] for entry in summary} == {
        issue: identities for issue, (_, identities) in RETAINED_EDITIONS.items()}
    assert {entry["table1_cells_evaluated"] for entry in summary} == {9}
    assert {entry["report_date"]: [(item["result_line"], item["measure"], item["difference"])
                                   for item in entry["identity_exceptions"]] for entry in summary} == {
        issue.isoformat(): [(11, "prior_fytd_budget_amt", "14258000.00")] if issue == date(2025, 9, 30) else []
        for issue in issues}
    march_2015 = {(row["src_line_nbr"], row["measure"]): row for row in rows if row["report_date"] == issues[0]}
    literal_null = march_2015[14, "next_year_budget_est_amt"]
    assert (literal_null["amount"], literal_null["amount_literal"], literal_null["marker_kind"]) == (
        None, "null", "publisher_null")
    september = {(row["src_line_nbr"], row["measure"]): row for row in rows if row["report_date"] == issues[-1]}
    estimate = september[2, "current_year_budget_est_amt"]
    assert (estimate["amount"], estimate["amount_unit"], estimate["amount_usd"], estimate["measure_fiscal_year"]) == (
        Decimal("5475705.00"), "usd_millions", Decimal("5475705000000.00"), 2026)
    assert september[2, "next_year_budget_est_amt"]["measure_fiscal_year"] == 2027
    prior = september[2, "prior_fytd_budget_amt"]
    assert (prior["amount"], prior["amount_unit"], prior["amount_usd"], prior["measure_fiscal_year"]) == (
        Decimal("5234616472030.91"), "usd", Decimal("5234616472030.91"), 2025)
    dots = september[8, "next_year_budget_est_amt"]
    assert (dots["amount"], dots["amount_usd"], dots["amount_literal"], dots["marker_kind"]) == (
        None, None, "0.00", "no_transactions")
    gap = {(row["src_line_nbr"], row["measure"]): row["amount"] for row in rows if row["report_date"] == issues[1]}
    means = sum(gap[line, "prior_fytd_budget_amt"] for line in (13, 14, 15))
    assert means - gap[11, "prior_fytd_budget_amt"] == Decimal("14258000.00")
    assert september[9, "current_month_budget_amt"]["parent_src_line_nbr"] == 8
    assert september[1, "current_month_budget_amt"]["parent_src_line_nbr"] is None


def _reviewed(monkeypatch, reader, key, entry):
    """Pin one review entry to a synthetic Table 2 page digest, as treasury_units.json would."""
    page = "sha256:" + hashlib.sha256(reader.body).hexdigest()
    monkeypatch.setitem(module.UNIT_RULES[key], page, entry)


@pytest.mark.parametrize(("kind", "amount"), [("zero", Decimal("0.00")), ("no_transactions", None)])
def test_a_zero_estimate_follows_its_editions_printed_page(tmp_path, monkeypatch, kind, amount):
    """A printed zero stays a zero; dots become No Transactions; nothing is decided from 0.00 alone."""
    cells = control()
    _table2_cell(cells, 14, "next_year_budget_est_amt", "0.00")
    _table2_cell(cells, 15, "next_year_budget_est_amt", "0.50")
    _reviewed(monkeypatch, cells, "zero_estimates", {"no_transactions": {}, "zero": {}} | {
        kind: {"next_year_budget_est_amt": [14]}})
    _, summary = module.build_treasury_fiscal(
        [debt(), statement(), cells], tmp_path, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
        qualifications={ISSUE: QUALIFICATION})
    row = next(row for row in pq.read_table(tmp_path / (module.CELLS + ".parquet")).to_pylist()
               if row["src_line_nbr"] == 14 and row["measure"] == "next_year_budget_est_amt")
    assert (row["marker_kind"], row["amount"], row["amount_usd"], row["amount_literal"]) == (
        kind, amount, amount, "0.00")
    assert summary[0]["identities"]["skipped"] == (kind == "no_transactions")


@pytest.mark.parametrize(("literal", "outcome"), [("0.35", "excepted"), ("0.45", "refused"), ("0.25", "unused")])
def test_identity_exception_is_pinned_to_its_page_and_exact_difference(tmp_path, monkeypatch, literal, outcome):
    """Line 15 is 0.25 when the identity holds; the pin allows exactly a 0.10 gap and nothing else."""
    cells = control()
    _table2_cell(cells, 15, "next_year_budget_est_amt", literal)
    _reviewed(monkeypatch, cells, "identity_exceptions", [{
        "result_line": 11, "operand_lines": [13, 14, 15], "measure": "next_year_budget_est_amt",
        "difference": "0.10", "evidence": "fixture page 5"}])
    if outcome != "excepted":
        with pytest.raises(ValueError, match="identity"):
            write([debt(), statement(), cells], tmp_path)
        return
    _, summary = module.build_treasury_fiscal(
        [debt(), statement(), cells], tmp_path, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
        qualifications={ISSUE: QUALIFICATION})
    assert summary[0]["identity_exceptions"] == [{"result_line": 11, "measure": "next_year_budget_est_amt",
                                                  "difference": "0.10", "evidence": "fixture page 5"}]
    row = next(row for row in pq.read_table(tmp_path / (module.CELLS + ".parquet")).to_pylist()
               if row["src_line_nbr"] == 15 and row["measure"] == "next_year_budget_est_amt")
    assert (row["amount"], row["amount_usd"]) == (Decimal("0.35"), Decimal("350000.00"))


def test_classification_ids_are_reassigned_per_edition_while_native_lines_hold():
    """Never join editions on classification_id: ids differ, the native line structure does not."""
    def structure(name):
        rows = json.loads((FIXTURES / (name + ".json")).read_bytes())["data"]
        lines = {row["classification_id"]: row["src_line_nbr"] for row in rows}
        return {row["classification_id"] for row in rows}, {
            row["src_line_nbr"]: (row["classification_desc"], row["sequence_number_cd"], row["sequence_level_nbr"],
                                  row["data_type_cd"], row["record_type_cd"], lines.get(row["parent_id"]),
                                  row["print_order_nbr"], row["line_code_nbr"]) for row in rows}
    march_ids, march = structure("mts_table_2-2026-03-31")
    september_ids, september = structure("mts_table_2-2026-09-30")
    assert not march_ids & september_ids
    assert march == september and len(march) == len(module.TABLE2_LAYOUT)


def test_decimal_arithmetic_keeps_cents_beyond_default_context_precision(tmp_path):
    source = debt()
    source.records[0]["record"]["raw"].update({
        "tot_pub_debt_out_amt": "9" * 36 + ".99", "debt_held_public_amt": "9" * 36 + ".98",
        "intragov_hold_amt": "0.01",
    })
    write([source, statement(), control()], tmp_path)
    row = pq.read_table(tmp_path / (module.DEBT + ".parquet")).to_pylist()[0]
    assert row["total_debt_usd"] == Decimal("9" * 36 + ".99")
    assert row["public_debt_usd"] == Decimal("9" * 36 + ".98")


def test_changed_report_control_refuses_before_writing_tables(tmp_path):
    report = tmp_path / "report.pdf"
    report.write_bytes(b"changed control")
    output = tmp_path / "build"
    with pytest.raises(ValueError, match="report control"):
        pipeline.build([debt(), statement(), control()], output, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
                       reports={ISSUE: QUALIFICATION | {"path": str(report)}})
    assert not (output / "generation").exists()
    assert not list(output.glob("*.parquet"))


def test_ordinary_generation_publication_and_local_query(tmp_path, monkeypatch):
    report = tmp_path / "control.pdf"
    report.write_bytes(b"%PDF synthetic report control")
    qualified = QUALIFICATION | {"path": str(report), "sha256": "sha256:" + hashlib.sha256(report.read_bytes()).hexdigest()}
    output = tmp_path / "build"
    artifact, evidence, _ = pipeline.build(
        [debt(), statement(), control()], output, debt_dates=(DEBT_DATE,), report_dates=(ISSUE,),
        reports={ISSUE: qualified},
    )
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["treasury-fiscal"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        values = connection.execute(f"""
            SELECT report_date, period_start, budget_receipts_usd, budget_outlays_usd, budget_deficit_usd,
                   json_extract_string(source_locator_json, '$.request_url')
            FROM {module.STATEMENTS} WHERE period_kind='month' AND economic_fiscal_year = 2026
        """).fetchall()
        cells = connection.execute(f"""
            SELECT c.measure, c.measure_fiscal_year, c.amount, c.amount_unit, p.budget_receipts_usd
            FROM {module.CELLS} c JOIN {module.STATEMENTS} p
              ON p.report_date = c.report_date AND p.period_kind = 'month'
             AND p.period_start = date_trunc('month', c.report_date)
            WHERE c.src_line_nbr = 2 AND c.measure = 'current_month_budget_amt'
        """).fetchall()
    assert values == [(ISSUE, date(2025, 10, 1), Decimal("1.01"), Decimal("2.02"), Decimal("1.01"),
                       "https://api.fiscaldata.treasury.gov/fixture")]
    assert cells == [("current_month_budget_amt", 2026, Decimal("1.01"), "usd", Decimal("1.01"))]
    for name, schema in module.SCHEMAS.items():
        assert pq.read_schema(output / (name + ".parquet")).equals(schema)


def test_acquire_records_receipt_scopes_and_rebuilds_identical_releases_from_raw(tmp_path):
    """Captures go to the raw archive once; each release is rebuilt from its descriptors alone."""
    bodies = {("mts_table_1", "2026-09-30"): "mts_table_1-2026-09-30", ("mts_table_2", "2026-09-30"):
              "mts_table_2-2026-09-30", ("debt_to_penny", "2026-10-16"): "debt_to_penny-2026-10-16"}
    report = b"%PDF-1.7 retained statement"
    listing = [{"report_date": "2026-09-30", "report_group_id": "5",
                "path": "/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf"}]
    wire_listing = gzip.compress(json.dumps(listing).encode(), mtime=0)
    requests = []

    def serve(request):
        requests.append(str(request.url))
        if request.url.path.endswith(".pdf"):
            return httpx.Response(200, content=report, headers={"content-type": "application/pdf"})
        if request.url.path.endswith("/publishedfiles"):
            # Fiscal Data serves the listing gzip-encoded; raw keeps those wire bytes.
            return httpx.Response(200, content=wire_listing,
                                  headers={"content-type": "application/json", "content-encoding": "gzip"})
        dataset = request.url.path.rsplit("/", 1)[-1]
        name = bodies[dataset, request.url.params["filter"].removeprefix("record_date:eq:")]
        return httpx.Response(200, content=(FIXTURES / (name + ".json")).read_bytes(),
                              headers={"content-type": "application/json"})

    selection = pipeline.acquire(
        [(dataset, date.fromisoformat(when)) for dataset, when in bodies], [date(2026, 9, 30)],
        tmp_path / "acquire", transport=httpx.MockTransport(serve))
    assert len(requests) == 5
    assert [scope["captures"][0]["response_sha256"] for scope in selection["releases"]] == [
        "sha256:" + hashlib.sha256((FIXTURES / (name + ".json")).read_bytes()).hexdigest() for name in bodies.values()]
    assert selection["reports"]["2026-09-30"]["sha256"] == "sha256:" + hashlib.sha256(report).hexdigest()
    (journal,) = (tmp_path / "acquire").glob("source-evidence/*/artifact/" + JOURNAL)
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    (listing_capture,) = [event for event in events
                          if event["event"] == "capture" and event["stage"] == pipeline.LISTING_STAGE]
    (stated,) = [event for event in events if event["event"] == "treasury-report-listing"]
    assert stated["listing_sha256"] == listing_capture["sha256"] == (
        "sha256:" + hashlib.sha256(wire_listing).hexdigest())
    assert listing_capture["content_encoding"] == "gzip"
    digests = []
    for run in ("first", "second"):
        artifact, _, summary = pipeline.build_from_captures(selection, tmp_path / run)
        digests.append(sorted(json.loads((root / "artifact.json").read_bytes())["artifactDigest"]
                              for root in (tmp_path / run / "source-releases").iterdir() if root.name.isdigit()))
        assert summary[0]["identities"] == {"evaluated": 28, "skipped": 12}
    assert digests[0] == digests[1] and len(digests[0]) == 3
    cells = pq.read_table(tmp_path / "first" / (module.CELLS + ".parquet")).to_pylist()
    assert {json.loads(row["source_locator_json"])["observed_at"] for row in cells} == {
        selection["releases"][1]["captures"][0]["observed_at"]}


def test_listed_debt_dates_are_captured_and_a_published_generation_names_its_own_captures(tmp_path):
    """A refresh needs no kept capture file: new debt days come from the listing, the rest from published rows."""
    report = b"%PDF-1.7 retained statement"
    listing = [{"report_date": "2026-09-30", "report_group_id": "5",
                "path": "/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf"}]
    requests = []

    def serve(request):
        requests.append(request.url.params.get("filter", request.url.path.rsplit("/", 1)[-1]))
        if request.url.path.endswith(".pdf"):
            return httpx.Response(200, content=report, headers={"content-type": "application/pdf"})
        if request.url.path.endswith("/publishedfiles"):
            return httpx.Response(200, json=listing)
        if request.url.params["filter"] == "record_date:gt:2026-10-15":
            assert (request.url.params["sort"], request.url.params["fields"]) == ("record_date", "record_date")
            return httpx.Response(200, json={"data": [{"record_date": "2026-10-16"}], "meta": {"total-count": 1}})
        dataset = request.url.path.rsplit("/", 1)[-1]
        name = f"{dataset}-{request.url.params['filter'].removeprefix('record_date:eq:')}"
        return httpx.Response(200, content=(FIXTURES / (name + ".json")).read_bytes(),
                              headers={"content-type": "application/json"})

    selection = pipeline.acquire(
        [("mts_table_1", date(2026, 9, 30)), ("mts_table_2", date(2026, 9, 30))], [date(2026, 9, 30)],
        tmp_path / "acquire", debt_after=date(2026, 10, 15), transport=httpx.MockTransport(serve))
    assert requests[0] == "record_date:gt:2026-10-15" and "record_date:eq:2026-10-16" in requests
    assert [(scope["selection"]["dataset"], scope["selection"]["record_date"]) for scope in selection["releases"]] == [
        ("mts_table_1", "2026-09-30"), ("mts_table_2", "2026-09-30"), ("debt_to_penny", "2026-10-16")]

    pipeline.build_from_captures(selection, tmp_path / "first")
    derived = pipeline.published_selection(sorted((tmp_path / "first" / "generation").glob("treasury_*.parquet")))
    assert sorted(derived["releases"], key=json.dumps) == sorted(selection["releases"], key=json.dumps)
    assert derived["reports"] == {"2026-09-30": {
        key: selection["reports"]["2026-09-30"][key] for key in ("url", "sha256")}}
    pipeline.build_from_captures(derived, tmp_path / "second")
    for name in module.SCHEMAS:
        first, second = (pq.read_table(tmp_path / run / "generation" / f"{name}.parquet").to_pylist()
                         for run in ("first", "second"))
        assert first == second and first

    def too_many(request):
        return httpx.Response(200, json={"data": [{"record_date": "2026-10-16"}], "meta": {"total-count": 101}})

    with pytest.raises(ValueError, match="more new debt dates than one run selects"):
        pipeline.acquire([], [], tmp_path / "long-gap", debt_after=date(2026, 5, 1), transport=httpx.MockTransport(too_many))
