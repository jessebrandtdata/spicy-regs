"""The SQL court comparison only ever accepts what the row comparison accepts, and refuses nothing itself.

Each case runs both routes against the same retained original, at the real comparison boundary of a real
conversion: the unchanged table, then tables with one changed cell, reordered rows, a repeated row, an added or
dropped row, a null made a value, and changed Arrow metadata or schema.
"""
from contextlib import contextmanager
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.conversion_reads import _FamilyReader
from tests.test_court_prepared_conversion import clusters
from tests.test_native_conversion import DOCKET, bucket as _bucket, convert, publish_old

bucket = _bucket


def damage(table, change, column):
    rows = table.to_pylist()
    if change == 'value':
        rows[0][column] = 'changed'
    elif change == 'null':
        rows[0][column] = None if rows[0][column] is not None else 'made-a-value'
    elif change == 'order':
        rows.reverse()
    elif change == 'repetition':
        rows[-1] = rows[0]
    elif change == 'extra':
        rows.append(rows[0])
    elif change == 'missing':
        rows.pop()
    elif change == 'metadata':
        return table.replace_schema_metadata({b'changed': b'yes'})
    elif change == 'schema':
        return table.cast(pa.schema([f.with_type(pa.large_string()) if f.name == column else f for f in table.schema]))
    return pa.Table.from_pylist(rows, schema=table.schema)


CHANGES = ('value', 'null', 'order', 'repetition', 'extra', 'missing', 'metadata', 'schema')


def verdicts(reader, dataset, original, monkeypatch, compare):
    """(what the SQL proof said or None if never asked, refusal message or None) for each route's whole comparison."""
    asked, real = [], _FamilyReader._bulk_court_equal

    def spy(*args, **kwargs):
        asked.append(real(*args, **kwargs))
        return asked[-1]

    def run():
        try:
            compare(reader, dataset, original)
        except ValueError as refused:
            return str(refused)

    with monkeypatch.context() as patch:
        patch.setattr(_FamilyReader, '_bulk_court_equal', spy)
        bulk = run()
        patch.setattr(_FamilyReader, '_bulk_court_equal', lambda *args, **kwargs: False)
        row = run()
    return (asked[0] if asked else None), bulk, row


@pytest.mark.parametrize('family, dataset, rows, column', [
    ('court-opinion-clusters', 'court_opinion_clusters',
     clusters() + [r | {'cluster_id': str(90 + n)} for n, r in enumerate(clusters())], 'case_name'),
    ('courtlistener', 'court_dockets', [DOCKET, DOCKET | {'cl_docket_id': '9', 'parties_json': None}], 'case_name'),
])
def test_bulk_and_row_comparisons_reach_the_same_verdict(tmp_path, monkeypatch, bucket, family, dataset, rows, column):
    publish_old(bucket, monkeypatch, tmp_path, family, {dataset: rows})
    compare, seen = _FamilyReader.compare_original, []

    def probe(reader, name, original, **kwargs):
        if not seen:
            table = pq.read_table(original)
            seen.append(('unchanged', *verdicts(reader, name, original, monkeypatch, compare)))
            for change in CHANGES:
                path = tmp_path / f'{change}.parquet'
                pq.write_table(damage(table, change, column), path)
                seen.append((change, *verdicts(reader, name, path, monkeypatch, compare)))
        return compare(reader, name, original, **kwargs)

    monkeypatch.setattr(_FamilyReader, 'compare_original', probe)
    convert(family, tmp_path / 'work')
    unchanged, *changed = seen
    assert unchanged == ('unchanged', True, None, None)
    assert [change for change, *_ in changed] == list(CHANGES)
    for change, proof, bulk, row in changed:
        # A retained schema is read from the original itself, so a different physical string type that restores
        # the same values is accepted by the row comparison; whatever it decides, the SQL route decides alike.
        assert (row is None) == (change == 'schema'), change
        assert bulk == row, change
        assert proof is not True or row is None, change  # The SQL proof accepts only what the row route accepts.
        assert proof is not True or change == 'schema', change


@contextmanager
def admitted_court(tmp_path, dataset, rows, original):
    """Exercise the real admitted reader without the family converter's fixed legacy string schema."""
    from spicy_regs.conversion_reads import _open_family_reader
    from spicy_regs.court_receipts import POLICIES, file_witness, local_receipt_selection, write_court_rows

    retained = tmp_path / 'original.parquet'
    pq.write_table(original, retained)
    subject = write_court_rows(dataset, rows, tmp_path / 'native', witnesses=[file_witness(retained)],
                               generation_id='comparison-test')
    receipts, generation = local_receipt_selection(subject)
    with _open_family_reader({dataset: [subject]}, [receipts], [POLICIES[dataset]],
                             generation_id=generation, guard=lambda: None) as reader:
        yield reader, retained


@pytest.mark.parametrize('attorneys', ['Counsel prose', ['Counsel', None, ''], None])
def test_cluster_attorneys_matches_the_row_decoder(tmp_path, monkeypatch, attorneys):
    dataset = 'court_opinion_clusters'
    row = {'cluster_id': '1', 'attorneys': attorneys}
    schema = pa.schema([('cluster_id', pa.string()),
                        ('attorneys', pa.string() if isinstance(attorneys, str) else pa.list_(pa.string()))])
    with admitted_court(tmp_path, dataset, [row], pa.Table.from_pylist([row], schema=schema)) as (reader, retained):
        proof, bulk, reference = verdicts(reader, dataset, retained, monkeypatch, _FamilyReader.compare_original)
        assert bulk == reference
        if isinstance(attorneys, str):
            assert proof is False
            assert reference == 'Restored processing values, order or repetitions differ from original'
        else:
            assert proof is True and reference is None


def test_docket_json_synthesis_uses_the_row_decoder(tmp_path, monkeypatch):
    dataset = 'court_dockets'
    row = {'cl_docket_id': '1', 'parties': ['Party', None, ''], 'attorneys': None, 'firms': []}
    original = pa.Table.from_pylist([{'cl_docket_id': '1', 'parties_json': json.dumps(row['parties']),
                                      'attorneys_json': None, 'firms_json': '[]'}],
                                    schema=pa.schema([(name, pa.string()) for name in
                                                      ('cl_docket_id', 'parties_json', 'attorneys_json', 'firms_json')]))
    with admitted_court(tmp_path, dataset, [row], original) as (reader, retained):
        proof, bulk, reference = verdicts(reader, dataset, retained, monkeypatch, _FamilyReader.compare_original)
        assert proof is True and bulk is None and reference is None


def test_only_unproven_court_rows_reach_python(tmp_path, monkeypatch):
    from spicy_regs import court_receipts

    rows = [clusters()[0] | {'cluster_id': str(i + 1), 'case_name': 'plain'} for i in range(73)]
    for i in (7, 62):
        rows[i]['case_name'] = 'escape\x1b'
    original = pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in rows[0]]))
    with admitted_court(tmp_path, 'court_opinion_clusters', rows, original) as (reader, retained):
        decoded, real = [], court_receipts.processing_rows
        subsets, subset = [], reader._court_subset_equal

        def observe_subset(*args):
            result = subset(*args)
            subsets.append((len(args[-1]), result))
            return result

        def observe(values, **kwargs):
            values = list(values)
            decoded.extend(row['cluster_id'] for row in values)
            yield from real(values, **kwargs)

        monkeypatch.setattr(court_receipts, 'processing_rows', observe)
        monkeypatch.setattr(reader, '_court_subset_equal', observe_subset)
        reader.compare_original('court_opinion_clusters', retained)
        assert decoded == ['8', '63'], (subsets, reader.row_reference)
        damaged = original.to_pylist()
        damaged[7]['case_name'] = 'different\x1b'
        pq.write_table(pa.Table.from_pylist(damaged, schema=original.schema), retained)
        proof, bulk, reference = verdicts(reader, 'court_opinion_clusters', retained, monkeypatch,
                                          _FamilyReader.compare_original)
        assert proof is False and bulk == reference
        assert reference == 'Restored processing values, order or repetitions differ from original'


@pytest.mark.parametrize('require_bulk', [False, True])
@pytest.mark.parametrize('failure_kind', ['duckdb', 'arrow'])
def test_court_comparison_propagates_bulk_execution_failure_without_row_replay(
    tmp_path, monkeypatch, require_bulk, failure_kind,
):
    import duckdb
    from spicy_regs import court_receipts, etl_bulk

    row = clusters()[0]
    rows = [row | {'cluster_id': str(i + 1)} for i in range(5)]
    original = pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in row]))
    failure = (duckdb.OutOfMemoryException('injected comparison memory failure') if failure_kind == 'duckdb'
               else pa.ArrowMemoryError('injected comparison allocation failure'))
    with admitted_court(tmp_path, 'court_opinion_clusters', rows, original) as (reader, retained):
        actual = reader.connection
        comparisons = []

        class BrokenConnection:
            def __getattr__(self, name):
                return getattr(actual, name)

            def execute(self, sql, *args, **kwargs):
                if sql.startswith('WITH s AS (SELECT'):
                    comparisons.append(True)
                    if len(comparisons) == 2:
                        raise failure
                return actual.execute(sql, *args, **kwargs)

        monkeypatch.setattr(etl_bulk, '_BATCH', 2)
        monkeypatch.setattr('spicy_regs.conversion_reads._GROUP_ROWS', 2)
        monkeypatch.setattr(reader, 'connection', BrokenConnection())
        monkeypatch.setattr(court_receipts, 'processing_rows',
                            lambda *a, **k: pytest.fail('operational failure must not start row replay'))
        with etl_bulk.bulk_limits(threads=1, memory_limit='256MB', require_bulk=require_bulk):
            with pytest.raises(etl_bulk.BulkValidationError, match='Court comparison proof failed') as raised:
                reader.compare_original('court_opinion_clusters', retained)
        assert raised.value.__cause__ is failure
        assert len(comparisons) == 2
