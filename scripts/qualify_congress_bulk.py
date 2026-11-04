"""Qualify full local Congress inputs without uploading or selecting production data.

Run from a gated checkout against hash-pinned full source files. This writes an
append-only phase log, private bundles, and an exact source/restore comparison.
Every reference route and a deterministic sample of each source shape is checked
against congress_attempt, the same row path bulk=False uses. The unchanged full
history join is measured and compared field for field. Index and upload/serving
remain separate measured phases supplied by their owning readers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sys
import time
from typing import Any
from contextlib import contextmanager
from itertools import zip_longest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import congress_bulk, etl_bulk
from spicy_regs.congress_receipts import PROCESSOR, _digest, policy
from spicy_regs.receipt_history import carry_receipt_history, history_query_threads
from spicy_regs.votes_batch_receipts import congress_attempt, restore_processing_input, write_congress_dataset
from spicy_regs.etl_receipts import ReceiptContext


@contextmanager
def measured(output: Path, name: str):
    before = time.monotonic()
    start = resource.getrusage(resource.RUSAGE_SELF)
    record: dict[str, Any] = {'phase': name, 'status': 'running'}
    try:
        yield record
        record['status'] = 'passed'
    except BaseException as error:
        record.update(status='failed', errorType=type(error).__name__, error=str(error))
        raise
    finally:
        stop = resource.getrusage(resource.RUSAGE_SELF)
        record.update(seconds=time.monotonic()-before, userCpuSeconds=stop.ru_utime-start.ru_utime,
                      systemCpuSeconds=stop.ru_stime-start.ru_stime, processPeakRss=stop.ru_maxrss,
                      processPeakRssUnit='bytes' if sys.platform == 'darwin' else 'KiB')
        with output.open('a') as sink:
            sink.write(json.dumps(record, sort_keys=True)+'\n')
        print(json.dumps(record, sort_keys=True), flush=True)


def compare_files(source: Path, restored: Path, *, processing_only: bool = False):
    expected, actual = pq.ParquetFile(source), pq.ParquetFile(restored)
    if not expected.schema_arrow.equals(actual.schema_arrow, check_metadata=True):
        raise ValueError('Restore schema or metadata differs from the original source')
    rows = 0
    columns = None
    if processing_only:
        columns = [name for name in expected.schema_arrow.names
                   if name not in {'receipt_id', 'witnesses', 'diagnostic_json'}]
    for a, b in zip_longest(expected.iter_batches(batch_size=10000), actual.iter_batches(batch_size=10000)):
        if a is not None and b is not None and columns is not None:
            a, b = a.select(columns), b.select(columns)
        if a is None or b is None or not a.equals(b):
            raise ValueError(f'Restored source differs at batch beginning at row {rows}')
        rows += a.num_rows
    if rows != expected.metadata.num_rows:
        raise ValueError('Full restore row count differs from source footer')
    return rows


def row_check(source: Path, receipts: Path, *, dataset: str, generation_id: str):
    selected = policy(dataset)
    digest = _digest(source)
    schema = pq.read_schema(source)
    reference = congress_bulk._context_reference(source.resolve(), dataset, selected, generation_id, PROCESSOR, ())
    raw = congress_bulk._source_sql(source.resolve(), schema, dataset, selected, reference)
    fields = ','.join(f'"{n}"' for n in schema.names)
    source_struct = 'struct_pack(' + ','.join(f'"{n}" := s."{n}"' for n in schema.names) + ')'
    # Congress/chamber, rejected/accepted shape, reference route. Always retain every routed ordinal.
    query = f'''
    WITH s AS ({raw}), ranked AS (SELECT *, row_number() OVER (
        PARTITION BY split_part(vote_id,'-',1), chamber, _rejected, _reference ORDER BY _ordinal) AS shape_row FROM s),
    samples AS (SELECT _ordinal FROM ranked WHERE shape_row<=8 OR _ordinal%100003=0 OR _reference),
    r AS (SELECT *, TRY_CAST(witnesses[1].locator AS BIGINT) AS ordinal FROM read_parquet(?))
    SELECT s._ordinal, {source_struct} AS raw, r AS receipt FROM s JOIN samples USING(_ordinal)
    LEFT JOIN r ON s._ordinal=r.ordinal ORDER BY s._ordinal'''
    checked, routed, outcomes = 0, 0, {}
    with etl_bulk.bulk_connection() as (con, _):
        # Finish no other statement while this result is being read.
        for batch in con.execute(query, [str(receipts)]).to_arrow_reader(2000):
            for held in batch.to_pylist():
                ordinal, row, receipt = held['_ordinal'], held['raw'], held['receipt']
                if receipt is None:
                    raise ValueError(f'Missing receipt at source ordinal {ordinal}')
                receipt.pop('ordinal')
                witness = {'source_id': 'shaped-observation:'+dataset, 'source_uri': str(source.resolve()),
                           'sha256': digest, 'locator': str(ordinal), 'body_version': None}
                context = ReceiptContext(generation_id, f'{dataset}:{digest}:{ordinal}', PROCESSOR, [witness])
                _, expected = congress_attempt(dataset, row, context)
                if receipt != expected:
                    raise ValueError(f'Bulk receipt differs from row path at source ordinal {ordinal}')
                checked += 1
                outcomes[receipt['outcome']] = outcomes.get(receipt['outcome'], 0)+1
        routed = sum(1 for _ in congress_bulk.iter_routed_ordinals(source, dataset=dataset, policy=selected,
                                                                  generation_id=generation_id, processor=PROCESSOR))
    return {'checked': checked, 'everyRoutedOrdinal': routed, 'sampleOutcomes': outcomes, 'strata':
            ['Congress', 'chamber', 'accepted/rejected source shape', 'reference route'], 'sourceColumns': fields}



def sample_writers(source: Path, output: Path, *, dataset: str, generation_id: str):
    """Compare complete row and bulk writers on a bounded stratified source sample.

    Full-file row_check separately retains original source ordinals and context.
    This sample checks both writers against exactly the same private source bytes.
    """
    selected, schema = policy(dataset), pq.read_schema(source)
    reference = congress_bulk._context_reference(source.resolve(), dataset, selected, generation_id, PROCESSOR, ())
    raw = congress_bulk._source_sql(source.resolve(), schema, dataset, selected, reference)
    names = ','.join('"'+name+'"' for name in schema.names)
    query = f"""WITH s AS ({raw}), ranked AS (SELECT *, row_number() OVER (
        PARTITION BY split_part(vote_id,'-',1), chamber, _rejected, _reference ORDER BY _ordinal) AS shape_row FROM s)
        SELECT {names} FROM ranked WHERE shape_row<=8 OR _ordinal%100003=0 ORDER BY _ordinal LIMIT 20001"""
    sample = output/'writer-sample.parquet'
    count = 0
    with etl_bulk.bulk_connection() as (con, _), pq.ParquetWriter(sample, schema, compression='zstd') as writer:
        for batch in con.execute(query).to_arrow_reader(2000):
            count += batch.num_rows
            if count > 20000:
                raise ValueError('Writer sample exceeds declared 20000-row bound')
            writer.write_table(pa.Table.from_batches([batch]).cast(schema))
    fast = write_congress_dataset(sample, output/'sample-bulk', dataset=dataset, generation_id=generation_id)
    from spicy_regs.congress_receipts import write_congress_dataset as current_row_writer
    row = current_row_writer(sample, output/'sample-row', dataset=dataset, generation_id=generation_id, bulk=False)
    for expected, actual in zip(row, fast):
        assert expected is not None and actual is not None
        compare_files(expected, actual)
    from spicy_regs.congress_receipts import CongressInput
    prior = CongressInput(row[0], row[1], generation_id)
    reference = current_row_writer(sample, output/'sample-row-prior', dataset=dataset,
                                   generation_id=generation_id+'-next', prior=prior, bulk=False)
    carried = write_congress_dataset(sample, output/'sample-bulk-prior', dataset=dataset,
                                     generation_id=generation_id+'-next', prior=prior)
    for expected, actual in zip(reference, carried):
        assert expected is not None and actual is not None
        compare_files(expected, actual)
    return {'sampleRows': count, 'bound': 20000, 'strata': ['Congress','chamber','accepted/rejected shape','reference route']}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--dataset', required=True, choices=['member_votes', 'member_vote_terms'])
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--generation', required=True)
    parser.add_argument('--history-query-threads', type=int, default=1,
                        help='Thread setting for the unchanged-history query (default: 1)')
    args = parser.parse_args()
    try:
        history_query_threads(args.history_query_threads)
    except ValueError as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    log = args.output/'phases.jsonl'
    original_source = args.source.resolve()
    snapshot = args.output/'pinned-input.parquet'
    with measured(log, 'input-pin') as record:
        hasher = hashlib.sha256()
        with original_source.open('rb') as stream, snapshot.open('xb') as frozen:
            for chunk in iter(lambda: stream.read(1024*1024), b''):
                hasher.update(chunk)
                frozen.write(chunk)
        digest = hasher.hexdigest()
        args.source = snapshot.resolve()
        if digest != args.sha256.removeprefix('sha256:'):
            raise ValueError('Source hash differs from the pinned member')
        record.update(sha256=digest, bytes=args.source.stat().st_size, rows=pq.read_metadata(args.source).num_rows,
                      originalSource=str(original_source), snapshot=str(args.source))
    with measured(log, 'write-and-admit') as record:
        subjects, receipts = write_congress_dataset(args.source, args.output/'bundle', dataset=args.dataset,
                                                    generation_id=args.generation)
        assert subjects is not None
        record.update(subjectBytes=subjects.stat().st_size, receiptBytes=receipts.stat().st_size)
    with measured(log, 'validate-full'):
        etl_bulk.validate_bundle({args.dataset:[subjects]}, [receipts], [policy(args.dataset)], generation_id=args.generation)
    with measured(log, 'independent-row-path') as record:
        record.update(row_check(args.source, receipts, dataset=args.dataset, generation_id=args.generation))
    with measured(log, 'complete-sample-writers') as record:
        record.update(sample_writers(args.source, args.output, dataset=args.dataset, generation_id=args.generation))
    with measured(log, 'restore-and-admit'):
        restored = restore_processing_input(subjects, receipts, args.output/'restored.parquet', dataset=args.dataset,
                                             generation_id=args.generation)
    with measured(log, 'compare-full-source-order') as record:
        record['rows'] = compare_files(args.source, restored)
    with etl_bulk.bulk_connection() as (con, _), measured(log, 'independent-populations') as record:
        counts = dict(con.execute('SELECT outcome,count(*) FROM read_parquet(?) GROUP BY outcome', [str(receipts)]).fetchall())
        source_count = pq.read_metadata(args.source).num_rows
        subject_count = pq.read_metadata(subjects).num_rows
        if counts.get('observed') != 1 or counts.get('accepted',0) != subject_count or sum(
            counts.get(outcome,0) for outcome in ['accepted','rejected','refused','error']) != source_count:
            raise ValueError('Independent source/subject/receipt populations differ')
        record.update(sourceRows=source_count, subjectRows=subject_count, receiptOutcomes=counts)
    prior_generation = args.generation + '-selected-prior'
    with measured(log, 'separate-prior-source-writer-and-admission') as record:
        prior_subjects, prior_receipts = write_congress_dataset(args.source, args.output/'selected-prior',
            dataset=args.dataset, generation_id=prior_generation)
        assert prior_subjects is not None
        record.update(generation=prior_generation, receiptSha256=_digest(prior_receipts),
                      receiptBytes=prior_receipts.stat().st_size, subjectRows=compare_files(subjects, prior_subjects))
    with measured(log, 'unchanged-history-full') as record:
        carried = carry_receipt_history(
            receipts, [prior_receipts], args.output/'carried-receipts.parquet',
            threads=args.history_query_threads, report_settings=record.update,
        )
        record.update(rows=compare_files(prior_receipts, carried), currentGeneration=args.generation,
                      selectedPriorGeneration=prior_generation, currentReceiptSha256=_digest(receipts),
                      selectedPriorReceiptSha256=_digest(prior_receipts), carriedReceiptSha256=_digest(carried))
        etl_bulk.validate_bundle({args.dataset:[subjects]}, [carried], [policy(args.dataset)], generation_id=args.generation)
    with measured(log, 'final-input-pin') as record:
        with args.source.open('rb') as stream:
            final_digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if final_digest != digest:
            raise ValueError('Pinned snapshot changed during qualification')
        with etl_bulk.bulk_connection() as (con, _):
            changed_witness = con.execute('SELECT count(*) FROM read_parquet(?) WHERE witnesses[1].sha256 IS DISTINCT FROM ?',
                                          [str(receipts), 'sha256:'+digest]).fetchone()
            if changed_witness != (0,):
                raise ValueError('Written source witnesses differ from the pinned snapshot')
        record['sha256'] = final_digest
    (args.output/'LIMITATIONS.json').write_text(json.dumps({'changedHistory': 'bounded regression only',
        'index': 'not measured by this command', 'upload': 'not measured by this command',
        'serving': 'not measured by this command', 'productionApproval': 'required separately'},indent=2)+'\n')


if __name__ == '__main__':
    main()
