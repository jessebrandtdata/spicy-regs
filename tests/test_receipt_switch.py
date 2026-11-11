"""The equality harness every family's switch off per-row receipts is proven with (``receipt_switch``, ss-ag1j).

Each check is shown to see the difference it exists for, on the federal-register fixture's real tables.
"""

import json
import operator
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.receipt_switch import promoted_differences, receipt_files, receipt_free_prior, table_differences

FIXTURE = Path(__file__).parent / "fixtures/federal_register_switch"
TABLE = FIXTURE / "expected/night-1.parquet"
KEY = ("document_number", "publication_date")


def _write(table, path):
    pq.write_table(table, path)
    return path


def test_an_identical_table_has_no_differences():
    assert table_differences(TABLE, TABLE, key=KEY) == []


def test_every_kind_of_difference_is_named(tmp_path):
    table = pq.read_table(TABLE)
    # A changed value: one row only in each table.
    titles = table["title"].to_pylist()
    titles[3] = "changed"
    changed = _write(table.set_column(table.schema.get_field_index("title"), "title", pa.array(titles)),
                     tmp_path / "changed.parquet")
    assert table_differences(changed, TABLE, key=KEY) == ["1 rows only in the first", "1 rows only in the second"]
    # The same rows in another order: only the positions differ.
    reordered = _write(table.take(list(range(1, table.num_rows)) + [0]), tmp_path / "reordered.parquet")
    assert table_differences(reordered, TABLE, key=KEY) == [f"{table.num_rows:,} positions hold a different key"]
    # A column type is a schema difference even where every value reads the same.
    widened = _write(table.cast(table.schema.set(table.schema.get_field_index("volume"),
                                                 pa.field("volume", pa.large_string()))), tmp_path / "widened.parquet")
    assert table_differences(widened, TABLE, key=KEY) == ["schema differs in a column type or nullability"]


def test_promoted_values_are_checked_against_the_receipts_copy(tmp_path):
    from spicy_regs.etl_receipts import receipt_policies
    from spicy_regs.transforms.regulations_receipts import policy

    manifest = json.loads((FIXTURE / "manifest.json").read_text())
    prior = FIXTURE / "prior/federal_register.parquet"
    [earlier] = [p for p in receipt_policies(policy("federal_register"))
                 if p.subject_schema.equals(pq.read_schema(prior))]
    copies = {name: operator.itemgetter(name) for name in ("html_url", "pdf_url", "body_html_url")}
    read = {(d["document_number"], d["publication_date"])
            for d in json.loads((FIXTURE / "pages/night-1.json").read_bytes())["results"]}
    carried = {(row["document_number"], row["publication_date"]) for row in pq.read_table(prior).to_pylist()} - read

    def check(table, among=carried):
        return promoted_differences(table, prior, FIXTURE / "prior/etl_receipts.parquet", earlier,
                                    generation_id=manifest["prior"]["generationId"], key=KEY, copies=copies,
                                    among=among)

    assert check(TABLE) == []
    table = pq.read_table(TABLE)
    urls = table["html_url"].to_pylist()
    rows = table.to_pylist()
    at = next(i for i, row in enumerate(rows) if (row["document_number"], row["publication_date"]) in carried)
    urls[at] = "https://example.invalid/"
    edited = _write(table.set_column(table.schema.get_field_index("html_url"), "html_url", pa.array(urls)),
                    tmp_path / "edited.parquet")
    [problem] = check(edited)
    assert "html_url: 'https://example.invalid/' != receipt" in problem
    dropped = _write(table.filter(pa.array([i != at for i in range(table.num_rows)])), tmp_path / "dropped.parquet")
    assert check(dropped) == [f"{(rows[at]['document_number'], rows[at]['publication_date'])} has a receipt and no row"]


def test_the_plain_route_is_handed_no_receipts_file_and_any_left_behind_is_found(tmp_path):
    prior = receipt_free_prior(FIXTURE / "prior", tmp_path / "prior")
    assert sorted(path.name for path in prior.iterdir()) == ["federal_register.parquet"]
    assert receipt_files(tmp_path) == []
    (tmp_path / "work").mkdir()
    shutil.copyfile(FIXTURE / "prior/etl_receipts.parquet", tmp_path / "work" / "etl_receipts.parquet")
    assert receipt_files(tmp_path) == [tmp_path / "work" / "etl_receipts.parquet"]


def test_a_key_that_does_not_name_one_row_is_refused(tmp_path):
    """Two rows sharing the key could trade places and still compare equal, so such a key is never accepted."""
    table = pq.read_table(TABLE)
    # Two rows with one publication date, swapped: keyed on the date alone, every position would still agree.
    dates = table["publication_date"].to_pylist()
    first = next(i for i in range(len(dates) - 1) if dates[i] == dates[i + 1])
    swapped = _write(table.take([*range(first), first + 1, first, *range(first + 2, table.num_rows)]),
                     tmp_path / "swapped.parquet")
    with pytest.raises(ValueError, match=r"\(publication_date\) names .* rows; compare on the whole row identity"):
        table_differences(swapped, TABLE, key=("publication_date",))
    assert table_differences(swapped, TABLE, key=KEY) == ["2 positions hold a different key"]
    # A duplicated row is refused the same way rather than counted.
    doubled = _write(pa.concat_tables([table, table.slice(0, 1)]), tmp_path / "doubled.parquet")
    with pytest.raises(ValueError, match="compare on the whole row identity"):
        table_differences(doubled, TABLE, key=KEY)
