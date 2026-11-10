"""Court preparation separates native domain types from exact original literals.

These ran on court-opinions until its receipts were retired (ss-vse3): a retired table never converts back into
receipts (``test_a_retired_court_table_never_converts_into_receipts``), so they run on court-opinion-clusters, which
still publishes receipts and has the same kinds of conversion (booleans, counts, receipt-only literals).
"""
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, BUCKET, MAIN, STATE, WHEEL, bucket as _bucket, convert, prepared_digest, public_rows, publish_old,
)

bucket = _bucket


def observed_processing(monkeypatch):
    """Observe exact literals and schema consumed by the real comparison stream."""
    from spicy_regs import court_receipts

    calls, processing = [], court_receipts.processing_rows

    def observed(*args, **kwargs):
        rows = list(processing(*args, **kwargs))
        calls.append((kwargs['schema'], rows))
        yield from rows

    monkeypatch.setattr(court_receipts, 'processing_rows', observed)
    return calls


@pytest.fixture(params=['bulk', 'row'])
def route(request, monkeypatch):
    """Run the comparison with its SQL proof, or with only the row comparison that has always decided."""
    from spicy_regs.conversion_reads import _FamilyReader

    verdicts, real = [], _FamilyReader._bulk_court_equal

    def spy(*args, **kwargs):
        verdicts.append(False if request.param == 'row' else real(*args, **kwargs))
        return verdicts[-1]

    monkeypatch.setattr(_FamilyReader, '_bulk_court_equal', spy)
    return request.param, verdicts


def assert_restored(route, compared, expected, work):
    """The SQL proof accepted every pass without the row stream, or the row stream restored the exact originals."""
    name, verdicts = route
    assert verdicts and len(verdicts) >= 1
    if name == 'bulk':
        assert all(verdicts) and not compared
        return
    assert not any(verdicts) and compared and all(rows == expected for _, rows in compared)
    assert all(schema.equals(pq.read_schema(work / 'retained/court_opinion_clusters.parquet'),
                             check_metadata=True) for schema, _ in compared)


def clusters():
    from spicy_regs.court_subjects import LEGACY_COLUMNS
    result = []
    for identity, boolean, count in [('7', 't', '007'), ('2', 'False', None)]:
        row = dict.fromkeys(LEGACY_COLUMNS['court_opinion_clusters'])
        row.update(cluster_id=identity, cl_docket_id='0004', precedential_status='Published',
                   blocked=boolean, citation_count=count, case_name=' literal  case ')
        result.append(row)
    return result


def test_original_court_clusters_prepare_and_publish_same_exact_native_artifact(tmp_path, monkeypatch, bucket, route):
    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': clusters()})
    work = tmp_path / 'prepared'
    compared = observed_processing(monkeypatch)
    prepared = convert('court-opinion-clusters', work)
    generation = Path(prepared['generation']['directory'])
    subject = pq.read_table(generation / 'court_opinion_clusters.parquet')
    assert subject['citation_count'].type == pa.int64() and subject['citation_count'].to_pylist() == [7, None]
    assert subject['blocked'].type == pa.bool_() and subject['blocked'].to_pylist() == [True, False]
    assert prepared['tables']['court_opinion_clusters']['type_changes'] == {}
    assert_restored(route, compared, clusters(), work)
    sealed = {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError('Prepared publication must reuse the qualified court generation')

    monkeypatch.setattr(conversion, '_convert_court', no_writer)
    published = conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinion-clusters'],
        expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE),
        expected_receipt_digest=prepared_digest(work / conversion.RECEIPT))
    assert published['generation'] == prepared['generation']
    assert public_rows(published) == {'court_opinion_clusters': 2}
    assert len(route[1]) == 1  # Only preparation restores: the seal's admission stands, public bytes are hashed.
    assert_restored(route, compared, clusters(), work)
    assert sealed == {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)['families']['court-opinion-clusters'] == old


def damaged(change, original):
    """The retained table as a dishonest original would be: one cell, the order, a repetition, or its metadata."""
    table = pq.read_table(original)
    rows = table.to_pylist()
    if change == 'metadata':
        return table.replace_schema_metadata({b'changed': b'yes'})
    if change == 'order':
        rows.reverse()
    elif change == 'value':
        rows[0]['citation_count'] = '7'
    elif change == 'repetition':
        rows[-1] = rows[0]
    elif change == 'extra':
        rows.append(rows[0])
    elif change == 'missing':
        rows.pop()
    else:
        raise AssertionError(change)
    return pa.Table.from_pylist(rows, schema=table.schema)


REFUSALS = {'order': 'values, order or repetitions', 'value': 'values, order or repetitions',
            'repetition': 'values, order or repetitions', 'extra': 'values, order or repetitions',
            'missing': 'Extra restored processing rows', 'metadata': 'schema or metadata'}


@pytest.mark.parametrize('change', list(REFUSALS))
def test_court_preparation_refuses_changed_original_processing_before_publication(
        tmp_path, monkeypatch, bucket, change, route):
    publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': clusters() + [row | {'cluster_id': row['cluster_id'] + '0'} for row in clusters()]})
    written = list(bucket.writes)
    intercepted = []
    from spicy_regs.conversion_reads import _FamilyReader
    compare = _FamilyReader.compare_original

    def changed(reader, dataset, original, **kwargs):
        intercepted.append(change)
        # Change the actual retained original at the comparison boundary. The real comparison must refuse it
        # before publication, whichever route proves it, with the row comparison's own message.
        pq.write_table(damaged(change, original), original)
        return compare(reader, dataset, original, **kwargs)

    monkeypatch.setattr(_FamilyReader, 'compare_original', changed)
    with pytest.raises(conversion.ConversionRefused, match=f'court_opinion_clusters does not restore.*{REFUSALS[change]}'):
        convert('court-opinion-clusters', tmp_path / 'prepared', publish=True)
    assert intercepted == [change]
    assert not any(route[1])  # The SQL proof never accepts a damaged original.
    assert bucket.writes == written


def large_processing_clusters():
    """A remaining receipt-only literal deliberately exercises the shrink guard."""
    import hashlib
    from spicy_regs.court_subjects import RECEIPT_FIELDS, SUBJECT_SCHEMAS

    assert 'slug' in RECEIPT_FIELDS['court_opinion_clusters']
    assert 'slug' not in SUBJECT_SCHEMAS['court_opinion_clusters'].names
    rows = clusters()
    for number, row in enumerate(rows):
        row['slug'] = ''.join(hashlib.sha256(f'{number}:{i}'.encode()).hexdigest() for i in range(300))
        row['case_name_full'] = f'Case {row["cluster_id"]} in full'
    return rows


@pytest.mark.parametrize('prepared_publication', [False, True])
def test_complete_conversion_replaces_size_heuristic_with_exact_restoration(
        tmp_path, monkeypatch, bucket, prepared_publication, route):
    monkeypatch.delenv('R2_ALLOW_SHRINK', raising=False)
    monkeypatch.delenv('R2_MIN_SIZE_RATIO', raising=False)
    rows = large_processing_clusters()
    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': rows})
    work = tmp_path / 'prepared'
    compared = observed_processing(monkeypatch)
    if prepared_publication:
        prepared = convert('court-opinion-clusters', work)
        generation = Path(prepared['generation']['directory'])
        before = list(bucket.writes)
        # Ordinary publication still refuses these same valid, smaller native bytes.
        with pytest.raises(RuntimeError, match='shrink'):
            publication.publish_generation(generation, client=bucket, bucket=BUCKET,
                                           prior_index=publication.current_index(BASE))
        assert bucket.writes == before
        sealed = {p.name: p.read_bytes() for p in generation.iterdir() if p.is_file()}
        receipt = conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinion-clusters'],
            expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE),
        expected_receipt_digest=prepared_digest(work / conversion.RECEIPT))
        assert sealed == {p.name: p.read_bytes() for p in generation.iterdir() if p.is_file()}
    else:
        receipt = convert('court-opinion-clusters', work, publish=True)
    entry = publication.current_index(BASE)['families']['court-opinion-clusters']
    assert entry['tables']['court_opinion_clusters.parquet']['byteSize'] < old['tables']['court_opinion_clusters.parquet']['byteSize'] / 2
    subject = pq.read_table(Path(receipt['generation']['directory']) / 'court_opinion_clusters.parquet')
    assert 'slug' not in subject.column_names
    assert subject['case_name_full'].to_pylist() == [row['case_name_full'] for row in rows]
    assert public_rows(receipt) == {'court_opinion_clusters': len(rows)}
    assert len(route[1]) == 1  # Preparation or same-process publication restores once; nothing is replayed.
    assert_restored(route, compared, rows, work)


@pytest.mark.parametrize('damage', ['saved-report', 'candidate', 'logical-id', 'prior', 'incomplete', 'extra'])
def test_publication_refuses_mismatched_or_saved_conversion_proof(tmp_path, monkeypatch, bucket, damage):
    import json
    from rulespec_artifacts import canonical_json_bytes

    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': large_processing_clusters()})
    work = tmp_path / 'prepared'
    convert('court-opinion-clusters', work)
    before = list(bucket.writes)
    publish = publication._publish_verified_generation
    intercepted = []

    def changed(*args, **kwargs):
        intercepted.append(damage)
        proof = kwargs['conversion_proof']
        if damage == 'saved-report':
            proof = json.loads((work / conversion.RECEIPT).read_text())
        elif damage == 'candidate':
            proof = proof._replace(artifact_digest='sha256:' + '00' * 32)
        elif damage == 'logical-id':
            proof = proof._replace(logical_id=old['logicalId'])
        elif damage == 'prior':
            proof = proof._replace(prior_entry=canonical_json_bytes(old | {'publishedAt': '2000-01-01T00:00:00Z'}))
        elif damage == 'incomplete':
            proof = proof._replace(restored_tables=frozenset())
        else:
            proof = proof._replace(restored_tables=proof.restored_tables | {'other.parquet'})
        return publish(*args, **kwargs | {'conversion_proof': proof})

    monkeypatch.setattr(publication, '_publish_verified_generation', changed)
    with pytest.raises(conversion.ConversionRefused, match='Native conversion proof differs'):
        conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinion-clusters'],
            expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE),
        expected_receipt_digest=prepared_digest(work / conversion.RECEIPT))
    assert intercepted == [damage]
    assert bucket.writes == before
    assert publication.current_index(BASE)['families']['court-opinion-clusters'] == old


def test_conversion_proof_cannot_be_reused_after_native_publication(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': large_processing_clusters()})
    publish = publication.publish_generation
    verified_publish = publication._publish_verified_generation
    proofs = []

    def capture(*args, **kwargs):
        proofs.append(kwargs['conversion_proof'])
        return verified_publish(*args, **kwargs)

    monkeypatch.setattr(publication, '_publish_verified_generation', capture)
    receipt = convert('court-opinion-clusters', tmp_path / 'work', publish=True)
    assert len(proofs) == 1
    before = list(bucket.writes)
    current = publication.current_index(BASE)['families']['court-opinion-clusters']
    with pytest.raises(publication.PublicationError, match='Native conversion proof differs'):
        publish(Path(receipt['generation']['directory']), client=bucket, bucket=BUCKET,
                prior_index=publication.current_index(BASE), conversion_proof=proofs[0])
    assert len(proofs) == 2
    assert bucket.writes == before
    assert publication.current_index(BASE)['families']['court-opinion-clusters'] == current


def test_conversion_keeps_complete_prior_on_conditional_pointer_retry(tmp_path, monkeypatch, bucket):
    from rulespec_artifacts import canonical_json_bytes

    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinion-clusters', {'court_opinion_clusters': large_processing_clusters()})
    changed = old | {'publishedAt': '2000-01-01T00:00:00Z'}

    def concurrent(key):
        if key == publication.INDEX_V2_KEY:
            current = publication.current_index(BASE)
            current['families']['court-opinion-clusters'] = changed
            bucket.objects[key] = canonical_json_bytes(current)

    bucket.before_put = concurrent
    with pytest.raises(conversion.ConversionRefused, match='changed|stale'):
        convert('court-opinion-clusters', tmp_path / 'work', publish=True)
    assert publication.current_index(BASE)['families']['court-opinion-clusters'] == changed


def test_a_retired_court_table_never_converts_into_receipts(tmp_path, monkeypatch, bucket):
    """court_opinions publishes no receipts (ss-vse3); converting a receiptless family would state them again."""
    from spicy_regs.court_subjects import LEGACY_COLUMNS
    from spicy_regs.subject_catalog import retired_receipt_datasets

    assert 'court_opinions' in retired_receipt_datasets()
    row = dict.fromkeys(LEGACY_COLUMNS['court_opinions']) | {'opinion_id': '7', 'cluster_id': '4'}
    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': [row]})
    before = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match=r"Receipts are retired for \['court_opinions'\]"):
        convert('court-opinions', tmp_path / 'work', publish=True)
    assert bucket.writes == before
    assert publication.current_index(BASE)['families']['court-opinions'] == old
