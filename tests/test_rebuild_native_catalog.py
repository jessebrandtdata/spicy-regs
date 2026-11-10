"""The comments v1 -> v2 catalog rebuild: guards, bulk copy, resumption and readback, on a local catalog."""

import duckdb
import hashlib
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, publication
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.etl_receipts import RECEIPT_SCHEMA

V2 = sorted(native.V2_COMMENT_COLUMNS)
SNAPSHOT = iceberg.CatalogPairSnapshot('local', 1, 0, iceberg.CatalogSnapshot('receipts', 2, 0))


def _connection():
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS reg_catalog")
    return connection


@pytest.fixture
def published(tmp_path):
    """A published v2 generation: subject and receipt Parquet exported from a v2 catalog."""
    rows = []
    for i, (docket, text) in enumerate([('A-1', 'first'), ('A-1', ' literal é😀 '), ('B-2', 'third'), ('C-3', None)]):
        row = {c: None for c in COMMENT.schema}
        row.update(comment_id=f'c{i}', docket_id=docket, agency_code='EPA', modify_date='2026-10-03', text_content=text,
                   text_extraction_status='complete', attachments_json='[]')
        rows.append(row)
    with _connection() as writer:
        writer.register('incoming', pl.DataFrame(rows, schema=COMMENT.schema).to_arrow())
        writer.execute('CREATE TEMP TABLE source AS SELECT * FROM incoming')
        iceberg.replace_rows(writer, COMMENT, 'source')
        return native.export_pair(writer, COMMENT, tmp_path / 'published', generation_id='comments-v2-fixture')


def _v1(published):
    """A live catalog in the 21-column v1 shape: same rows and receipts, a stale marker, no v2 columns."""
    con = _connection()
    names = [c for c in native.policy('comments').subject_schema.names if c not in native.V2_COMMENT_COLUMNS]
    schema = native.policy('comments').subject_schema
    v1 = pa.schema([f for f in schema if f.name in names])
    con.execute(f'CREATE SCHEMA reg_catalog."{native.namespace()}"')
    con.execute(f'CREATE TABLE {native.receipts_table()} ({native._ddl(RECEIPT_SCHEMA)})')
    con.execute(f'CREATE TABLE {native.qualified(COMMENT)} ({native._ddl(v1)})')
    columns = ', '.join(f'"{n}"' for n in names)
    con.execute(f'INSERT INTO {native.qualified(COMMENT)} SELECT {columns} FROM read_parquet(?)', [str(published.subjects[0])])
    con.execute(f"INSERT INTO {native.receipts_table()} SELECT * REPLACE ('regulations-native-v1' AS policy_version) "
                'FROM read_parquet(?)', [str(published.receipts)])
    return con


@pytest.fixture
def state(monkeypatch):
    held = {'snapshot': SNAPSHOT}
    monkeypatch.setattr(iceberg, '_read_pair_snapshot', lambda con, rt, namespace=None: held['snapshot'])
    return held


@pytest.fixture
def live(published, state):
    with _v1(published) as con:
        yield con


def rebuild(con, published, **overrides):
    arguments = dict(expected_snapshot=SNAPSHOT, expected_prior=published.subjects[0])
    return native.rebuild_native(con, COMMENT, published, **{**arguments, **overrides})


def untouched(con):
    return (con.execute(f'DESCRIBE {native.qualified(COMMENT)}').fetchall(),
            con.execute(f'SELECT count(*) FROM {native.qualified(COMMENT)}').fetchone(),
            con.execute(f'SELECT * FROM {native.receipts_table()} ORDER BY receipt_id').fetchall())


def test_rebuild_copies_published_rows_and_receipts_unchanged(live, published, tmp_path):
    assert native.rebuild_state(live, COMMENT) == 'v1'
    plan = rebuild(live, published)
    assert plan['rows'] == 4 and plan['state'] == 'v1'
    assert native.rebuild_state(live, COMMENT) == 'done'
    native.require_initialized(live, COMMENT)
    table, source = native.qualified(COMMENT), f"read_parquet('{published.subjects[0]}')"
    assert len(live.execute(f'DESCRIBE {table}').fetchall()) == 24
    assert live.execute(f'SELECT count(*) FROM (SELECT * FROM {table} EXCEPT ALL SELECT * FROM {source}) ').fetchone() == (0,)
    assert live.execute(f'SELECT count(*) FROM (SELECT * FROM {source} EXCEPT ALL SELECT * FROM {table})').fetchone() == (0,)
    names = ', '.join(RECEIPT_SCHEMA.names)
    carried = f"SELECT {names} FROM {native.receipts_table()} WHERE outcome='accepted'"
    kept = f"SELECT {names} FROM read_parquet('{published.receipts}') WHERE dataset='comments' AND outcome='accepted'"
    assert live.execute(f'SELECT count(*) FROM (({carried} EXCEPT ALL {kept}) UNION ALL ({kept} EXCEPT ALL {carried}))').fetchone() == (0,)
    assert live.execute(f"SELECT count(*) FROM {native.receipts_table()} WHERE processor='regulatory-catalog-initialization-v1'").fetchone() == (1,)
    result = native.verify_rebuilt(live, COMMENT, published)
    assert result['rows'] == 4 and result['smoke_docket'] in {'B-2', 'C-3'}
    with pytest.raises(ValueError, match='already rebuilt'):
        rebuild(live, published)


def test_dry_run_reads_and_checks_but_writes_nothing(live, published):
    before = untouched(live)
    plan = rebuild(live, published, dry_run=True)
    assert plan['dry_run'] and plan['live_rows'] == 4 and plan['target'].endswith('"comments"')
    assert untouched(live) == before and native.rebuild_state(live, COMMENT) == 'v1'
    live.execute(f"UPDATE {native.qualified(COMMENT)} SET text_content='drifted' WHERE comment_id='c0'")
    with pytest.raises(RuntimeError, match='1 mismatched'):
        rebuild(live, published, dry_run=True)


def test_refuses_live_columns_that_differ(live, published):
    live.execute(f'ALTER TABLE {native.qualified(COMMENT)} ADD COLUMN surprise VARCHAR')
    before = untouched(live)
    with pytest.raises(ValueError, match='neither the v1 nor the declared v2 shape'):
        rebuild(live, published)
    assert untouched(live) == before


@pytest.mark.parametrize('damage', ['changed-value', 'extra-row', 'missing-row'])
def test_refuses_live_rows_that_differ_from_published(live, published, damage):
    table = native.qualified(COMMENT)
    if damage == 'changed-value':
        live.execute(f"UPDATE {table} SET agency_code='DOE' WHERE comment_id='c3'")
    elif damage == 'extra-row':
        live.execute(f"INSERT INTO {table} SELECT * FROM {table} WHERE comment_id='c1'")
    else:
        live.execute(f"DELETE FROM {table} WHERE comment_id='c2'")
    before = untouched(live)
    with pytest.raises(RuntimeError, match='differs from the published subject'):
        rebuild(live, published)
    assert untouched(live) == before


@pytest.mark.parametrize('snapshots', [None, 'other', 'moves-after-guard'])
def test_refuses_wrong_or_moved_snapshot_without_writing(live, state, published, snapshots, monkeypatch):
    before = untouched(live)
    if snapshots is None:
        with pytest.raises(ValueError, match='requires the catalog snapshot'):
            rebuild(live, published, expected_snapshot=None)
    elif snapshots == 'other':
        state['snapshot'] = iceberg.CatalogPairSnapshot('local', 9, 0, SNAPSHOT.receipts)
        with pytest.raises(RuntimeError, match='snapshot changed'):
            rebuild(live, published)
    else:
        seen = iter([SNAPSHOT, iceberg.CatalogPairSnapshot('local', 9, 0, SNAPSHOT.receipts)])
        monkeypatch.setattr(iceberg, '_read_pair_snapshot', lambda con, rt, namespace=None: next(seen))
        with pytest.raises(RuntimeError, match='snapshot changed'):
            rebuild(live, published)
    assert untouched(live) == before


def test_refuses_a_published_pair_whose_receipts_do_not_match_its_rows(live, published, tmp_path):
    short = tmp_path / 'short-receipts.parquet'
    live.execute(f"COPY (SELECT * FROM read_parquet('{published.receipts}') WHERE record_id IS DISTINCT FROM "
                 f"(SELECT min(record_id) FROM read_parquet('{published.receipts}'))) TO '{short}' (FORMAT PARQUET)")
    from dataclasses import replace
    before = untouched(live)
    with pytest.raises(ValueError, match='accepted comments rows'):
        rebuild(live, replace(published, receipts=short))
    assert untouched(live) == before


def test_refuses_a_different_expected_prior_before_dropping_the_live_table(live, published, tmp_path):
    import shutil
    other = tmp_path / 'other-prior.parquet'
    shutil.copyfile(published.subjects[0], other)
    before = untouched(live)
    with pytest.raises(ValueError, match='same published subject'):
        rebuild(live, published, expected_prior=other)
    assert untouched(live) == before


def test_refuses_a_different_dataset_or_incomplete_receipt_schema_before_writing(live, published, tmp_path):
    from dataclasses import replace
    before = untouched(live)
    with pytest.raises(ValueError, match='another dataset'):
        rebuild(live, replace(published, dataset='documents'))
    missing = tmp_path / 'missing-receipt-field.parquet'
    native._copy(live, 'SELECT * EXCLUDE (witnesses) FROM read_parquet(?)', missing,
                 parameters=[str(published.receipts)])
    with pytest.raises(ValueError, match='receipt schema'):
        rebuild(live, replace(published, receipts=missing))
    assert untouched(live) == before


def test_refuses_partial_copies(live, published):
    rebuild(live, published)
    live.execute(f'DELETE FROM {native.receipts_table()} WHERE processor=\'regulatory-catalog-initialization-v1\'')
    assert_partial = pytest.raises(ValueError, match='partial copy')
    with assert_partial:
        rebuild(live, published)
    live.execute(f'DROP TABLE {native.qualified(COMMENT)}')
    with pytest.raises(ValueError, match='without a catalog table'):
        rebuild(live, published)


def test_interrupted_final_transaction_leaves_an_empty_table_and_a_rerun_resumes(live, published, monkeypatch):
    real = native._initialization_marker
    monkeypatch.setattr(native, '_initialization_marker', lambda *_: (_ for _ in ()).throw(OSError('disk')))
    with pytest.raises(OSError):
        rebuild(live, published)
    monkeypatch.setattr(native, '_initialization_marker', real)
    assert native.rebuild_state(live, COMMENT) == 'empty'
    assert live.execute(f'SELECT count(*) FROM {native.qualified(COMMENT)}').fetchone() == (0,)
    assert rebuild(live, published, expected_snapshot=None)['state'] == 'empty'
    native.verify_rebuilt(live, COMMENT, published)


def test_rerun_resumes_when_the_table_was_dropped(live, published):
    live.execute(f'DELETE FROM {native.receipts_table()} WHERE dataset=\'comments\'')
    live.execute(f'DROP TABLE {native.qualified(COMMENT)}')
    assert rebuild(live, published, expected_snapshot=None)['state'] == 'absent'
    native.require_initialized(live, COMMENT)


@pytest.mark.parametrize('damage', ['row', 'receipt', 'marker-missing'])
def test_readback_catches_a_corrupted_copy(live, published, tmp_path, damage):
    rebuild(live, published)
    if damage == 'row':
        live.execute(f"UPDATE {native.qualified(COMMENT)} SET text_content='tampered' WHERE comment_id='c0'")
    elif damage == 'receipt':
        live.execute(f"UPDATE {native.receipts_table()} SET subject_version='0'*64 WHERE outcome='accepted' "
                     f"AND record_id=(SELECT record_id FROM {native.receipts_table()} WHERE outcome='accepted' AND identity_json LIKE '%c0%')")
    else:
        live.execute(f"DELETE FROM {native.receipts_table()} WHERE processor='regulatory-catalog-initialization-v1'")
    with pytest.raises((ValueError, RuntimeError)) as refused:
        native.verify_rebuilt(live, COMMENT, published)
    assert str(refused.value)


def test_readback_refuses_valid_replacement_receipt_with_changed_provenance(live, published, tmp_path):
    from spicy_regs.etl_receipts import rebind_receipt
    rebuild(live, published)
    original = live.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted' AND identity_json LIKE '%c0%'").to_arrow_table().to_pylist()[0]
    changed = rebind_receipt(original, generation_id='another-generation')
    assert changed['record_id'] == original['record_id'] and changed['subject_version'] == original['subject_version']
    live.register('replacement', pa.Table.from_pylist([changed], schema=RECEIPT_SCHEMA))
    live.execute(f'DELETE FROM {native.receipts_table()} WHERE receipt_id=?', [original['receipt_id']])
    live.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM replacement')
    with pytest.raises(RuntimeError, match='receipts differ from the published generation'):
        native.verify_rebuilt(live, COMMENT, published)


@pytest.mark.parametrize('dockets', [[], [None], [None] * 101])
def test_readback_smoke_accepts_empty_or_unknown_docket_population(tmp_path, state, dockets):
    with _connection() as writer:
        rows = [{**dict.fromkeys(COMMENT.schema), 'comment_id': f'c{i}', 'docket_id': docket}
                for i, docket in enumerate(dockets)]
        writer.register('source', pl.DataFrame(rows, schema=COMMENT.schema).to_arrow())
        native.ensure_native(writer, COMMENT)
        iceberg.replace_rows(writer, COMMENT, 'source')
        pair = native.export_pair(writer, COMMENT, tmp_path / 'published', generation_id='unknown-docket')
    with _v1(pair) as con:
        rebuild(con, pair)
        result = native.verify_rebuilt(con, COMMENT, pair)
    assert result['rows'] == len(dockets)
    if len(dockets) > 100:
        assert result['smoke_skipped'] == 'smallest docket exceeds 100 rows'
    elif dockets:
        assert 'smoke_docket' in result and result['smoke_docket'] is None


@pytest.mark.parametrize('column', V2)
def test_readback_compares_every_new_comment_column(live, published, column):
    rebuild(live, published)
    live.execute(f'UPDATE {native.qualified(COMMENT)} SET "{column}"=? WHERE comment_id=?', ['tampered', 'c2'])
    with pytest.raises(RuntimeError, match='Sampled subjects differ'):
        native.verify_rebuilt(live, COMMENT, published, smoke=False)


@pytest.mark.parametrize('damage', ['subject-missing', 'receipt-missing', 'subject-schema', 'receipt-schema'])
def test_readback_checks_counts_and_schemas_beyond_sample(live, published, damage):
    rebuild(live, published)
    if damage == 'subject-missing':
        live.execute(f"DELETE FROM {native.qualified(COMMENT)} WHERE comment_id='c1'")
    elif damage == 'receipt-missing':
        live.execute(f"DELETE FROM {native.receipts_table()} WHERE identity_json LIKE '%c1%'")
    elif damage == 'subject-schema':
        live.execute(f'ALTER TABLE {native.qualified(COMMENT)} DROP COLUMN text_extraction_status')
    else:
        live.execute(f'ALTER TABLE {native.receipts_table()} ADD COLUMN extra VARCHAR')
    with pytest.raises((ValueError, RuntimeError)):
        native.verify_rebuilt(live, COMMENT, published, smoke=False)


def test_readback_samples_nonaccepted_provenance_and_scopes_receipt_counts(live, published, tmp_path):
    from dataclasses import replace
    from spicy_regs.etl_receipts import ReceiptContext, failure_receipt
    witnesses = pq.read_table(published.receipts).to_pylist()[0]['witnesses']
    failed = failure_receipt(native.policy('comments'), ReceiptContext('generation', 'attempt', 'reader', witnesses),
                             outcome='refused', raw_fields={'raw_source_record': {'comment_id': 'bad'}})
    other = {**failed, 'dataset': 'comments_index'}
    mixed = tmp_path / 'mixed-receipts.parquet'
    pq.write_table(pa.concat_tables([pq.read_table(published.receipts),
                                    pa.Table.from_pylist([failed, other], schema=RECEIPT_SCHEMA)]), mixed)
    pair = replace(published, receipts=mixed)
    rebuild(live, pair)
    live.register('other_receipt', pa.Table.from_pylist([other], schema=RECEIPT_SCHEMA))
    live.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM other_receipt')
    result = native.verify_rebuilt(live, COMMENT, pair, smoke=False)
    assert result['verification'] == 'sampled'
    assert result['sampled_subjects'] == 3 and result['sampled_receipts'] == 4
    assert result['receipts'] == 6  # Four accepted comments, one refusal, one checked marker; not comments_index.
    live.execute(f"UPDATE {native.receipts_table()} SET diagnostic_json=? WHERE dataset='comments' AND outcome='refused'",
                 ['{"changed":true}'])
    with pytest.raises(RuntimeError, match='including provenance'):
        native.verify_rebuilt(live, COMMENT, pair, smoke=False)


def test_readback_does_not_export_or_build_a_second_validation_database(live, published, monkeypatch):
    from spicy_regs import etl_bulk
    rebuild(live, published)
    monkeypatch.setattr(native, '_copy', lambda *a, **k: pytest.fail('must not export the catalog'))
    monkeypatch.setattr(etl_bulk, 'validate_bundle', lambda *a, **k: pytest.fail('must not validate the full catalog'))
    result = native.verify_rebuilt(live, COMMENT, published, smoke=False)
    assert result['verification'] == 'sampled' and result['sampled_subjects'] == 3



def test_smoke_count_and_processing_read_share_one_transaction(live, published, monkeypatch):
    rebuild(live, published)
    state = {'transaction': None, 'count_snapshot': None}
    class Connection:
        def execute(self, query, *args):
            if query == 'BEGIN':
                state['transaction'] = object()
            elif query in ('COMMIT', 'ROLLBACK'):
                state['transaction'] = None
            elif query.startswith('SELECT docket_id, count(*)'):
                state['count_snapshot'] = state['transaction']
            return live.execute(query, *args)
        def __getattr__(self, name):
            return getattr(live, name)
    original = native.processing_table
    def restore(con, record_type, **kwargs):
        assert kwargs['in_transaction'] is True
        assert state['transaction'] is not None and state['transaction'] is state['count_snapshot']
        return original(con, record_type, **kwargs)
    monkeypatch.setattr(native, 'processing_table', restore)
    assert 'smoke_docket' in native.verify_rebuilt(Connection(), COMMENT, published)


@pytest.fixture
def command(live, published, state, monkeypatch, tmp_path):
    """scripts/rebuild_native_catalog.py against the local catalog and a stubbed publication index."""
    import shutil
    from scripts import rebuild_native_catalog as script

    digest = 'sha256:08e42ccc' + '0' * 56
    def member(path):
        return publication.Member(path.name, 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_size, 4)
    monkeypatch.setenv('R2_PUBLIC_URL', 'https://example.invalid')
    monkeypatch.setattr(iceberg, 'is_configured', lambda: True)
    monkeypatch.setattr(publication, 'load_index', lambda base: {})
    monkeypatch.setattr(publication, 'table_owner', lambda index, key: ('comments', {
        'artifactDigest': digest, 'etlReceipts': {'datasets': ['comments', 'comments_index']}}))
    monkeypatch.setattr(publication, 'single_member', lambda index, key: member(published.subjects[0]))
    monkeypatch.setattr(publication, 'receipt_members', lambda index, dataset: (member(published.receipts),))
    monkeypatch.setattr(publication, 'fetch_member', lambda base, m, destination, label=None: shutil.copyfile(
        published.subjects[0] if m.path == published.subjects[0].name else published.receipts, destination))
    class Connection:  # The script closes its connection; the test keeps using the live one.
        def __getattr__(self, name):
            return getattr(live, name)
        def close(self):
            pass
    monkeypatch.setattr(iceberg, '_connect', lambda: Connection())
    return lambda *argv: script.main(['--work', str(tmp_path / 'work'), '--expect-artifact-digest', '08e42ccc0000', *argv])


def test_command_dry_run_prints_the_plan_and_writes_nothing(command, live, capsys):
    before = untouched(live)
    assert command() == 0
    out = capsys.readouterr().out
    assert 'rows 4' in out and 'missing input bytes' in out and 'not reserved' in out and 'default_native' in out and 'dry run' in out
    assert untouched(live) == before
    assert command('--dry-run') == 0 and untouched(live) == before


def test_command_publish_rebuilds_and_reads_back_and_verify_only_rechecks(command, live, capsys):
    assert command('--publish') == 0
    assert 'Sampled readback passed' in capsys.readouterr().out
    assert native.rebuild_state(live, COMMENT) == 'done'
    assert command('--verify-only') == 0


def test_command_refuses_other_generations_and_conflicting_flags(command, live):
    with pytest.raises(SystemExit):
        command('--publish', '--dry-run')
    from scripts import rebuild_native_catalog as script
    with pytest.raises(SystemExit, match='is not'):
        script.main(['--work', 'unused', '--expect-artifact-digest', 'ffffffffffff'])


@pytest.mark.parametrize('expected', ['08e42ccc0000', 'ffffffffffff'], ids=['archive-digest', 'live-digest'])
def test_command_refuses_a_receipts_free_live_generation_and_names_the_archive(command, live, monkeypatch, expected):
    """After the first receipts-free comments publication the live family has no receipt member to copy (ss-juz8)."""
    from scripts import rebuild_native_catalog as script
    monkeypatch.setattr(publication, 'table_owner', lambda index, key: ('comments', {'artifactDigest': 'sha256:' + 'f' * 64}))
    monkeypatch.setattr(publication, 'receipt_members', lambda *a, **kw: pytest.fail('read receipts it does not have'))
    before = untouched(live)
    with pytest.raises(SystemExit) as refusal:
        script.main(['--work', 'unused', '--expect-artifact-digest', expected])
    message = str(refusal.value)
    assert 'has no receipts' in message and script.LAST_RECEIPT_GENERATION in message
    assert 'docs/comments-receipt-archive.md' in message and 'is not built' in message
    assert untouched(live) == before


def test_command_reuses_verified_inputs_on_rerun_and_refuses_changed_bytes(command, monkeypatch, tmp_path):
    assert command() == 0
    monkeypatch.setattr(publication, 'fetch_member', lambda *_: pytest.fail('must reuse verified input'))
    assert command() == 0
    subject = tmp_path / 'work' / 'published' / 'comments.parquet'
    subject.write_bytes(b'changed by another task')
    with pytest.raises(ValueError, match='left untouched'):
        command()
    assert subject.read_bytes() == b'changed by another task'


def test_command_reuses_explicit_corpus_members_in_place(command, published, monkeypatch, tmp_path):
    monkeypatch.setattr(publication, 'fetch_member', lambda *_: pytest.fail('must not download retained inputs'))
    assert command('--subject-file', str(published.subjects[0]), '--receipts-file', str(published.receipts)) == 0
    assert list((tmp_path / 'work' / 'published').iterdir()) == []


def test_command_passes_resources_and_work_volume_to_nested_validation(command, monkeypatch, tmp_path):
    import tempfile
    from spicy_regs import etl_bulk
    previous = tempfile.tempdir
    real = native.verify_rebuilt
    def verify(*args, **kwargs):
        assert tempfile.tempdir == str(tmp_path / 'work' / 'temporary')
        limits = etl_bulk._limits.get()
        assert limits is not None and limits.threads == 2
        assert limits.memory_limit == '1GB'
        return real(*args, **kwargs)
    monkeypatch.setattr(native, 'verify_rebuilt', verify)
    assert command('--publish', '--threads', '2', '--memory', '1GB') == 0
    assert tempfile.tempdir == previous and etl_bulk._limits.get() is None



def test_command_does_not_reserve_the_spill_cap(command, published, monkeypatch, capsys):
    from types import SimpleNamespace
    from scripts import rebuild_native_catalog as script
    missing = published.subjects[0].stat().st_size + published.receipts.stat().st_size
    monkeypatch.setattr(script.shutil, 'disk_usage', lambda _: SimpleNamespace(free=missing))
    assert command('--spill', '64GiB') == 0
    output = capsys.readouterr().out
    assert 'configured spill cap 64GiB (not reserved)' in output and 'peak disk use is unmeasured' in output


def test_command_requires_space_only_for_missing_inputs_before_download(command, monkeypatch):
    from types import SimpleNamespace
    from scripts import rebuild_native_catalog as script
    monkeypatch.setattr(script.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))
    monkeypatch.setattr(publication, 'fetch_member', lambda *a: pytest.fail('must refuse before downloading'))
    with pytest.raises(SystemExit, match='missing published inputs'):
        command()
