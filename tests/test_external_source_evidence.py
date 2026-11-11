"""Exact native bytes, bounded retention, failure preservation and observation lineage."""
from __future__ import annotations

import gzip
import hashlib
import importlib
import io
import json
from datetime import datetime

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource, SourceEvidenceError, verify_evidence
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.sources.crs_reports import CrsReportsReader
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.sources.courtlistener import CourtListenerReader

usa = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")
sam = importlib.import_module("spicy_regs.transforms.build_sam_entities")


def journal(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def captures(evidence):
    return [e for e in journal(evidence) if e['event'] == 'capture']


def payload(evidence, capture):
    with EvidenceMemberSource(evidence.artifact_dir).open('blobs/sha256/' + capture['sha256'][7:]) as stream:
        return stream.read()


def stages(evidence):
    return {e['stage'] for e in captures(evidence)}


def assert_no_secret(evidence, secret):
    assert all(secret.encode() not in p.read_bytes() for p in evidence.directory.rglob('*') if p.is_file())


def _owner_capture(body):
    from spicy_docs.transport.captured import CapturedBodyResponse

    return CapturedBodyResponse('https://native.test/', 'https://native.test/', 200, 'application/json',
                                '2026-09-25T00:00:00Z', body)


@pytest.fixture
def tee(monkeypatch):
    """Route every tee a transform creates on ``evidence`` through ``respond``, as the real owner would see it."""
    def install(evidence, respond):
        original = evidence.transport
        monkeypatch.setattr(evidence, 'transport',
                            lambda *args, **kwargs: original(httpx.MockTransport(respond), **kwargs))
    return install


class Chunks(httpx.SyncByteStream):
    def __init__(self, parts):
        self.parts = parts

    def __iter__(self):
        yield from self.parts


def test_streamed_capture_retains_wire_bytes_request_and_retry_status(tmp_path):
    evidence = CaptureEvidence(tmp_path, 'test')
    attempts = []
    compressed = gzip.compress(b'{"native": "body"}')

    def respond(request):
        attempts.append(request)
        return httpx.Response(503 if len(attempts) == 1 else 200,
                              headers={'Content-Encoding': 'gzip'}, stream=Chunks([compressed[:7], compressed[7:]]))

    with httpx.Client(transport=evidence.transport(httpx.MockTransport(respond), stage='native', max_bytes=1000)) as client:
        for _ in range(2):
            assert client.post('https://native.test/', content=b'{"page":1}').content == b'{"native": "body"}'
    recorded = captures(evidence)
    assert [r['status_code'] for r in recorded] == [503, 200]
    assert all(payload(evidence, r) == compressed for r in recorded)
    assert all(r['request_body']['sha256'] == 'sha256:' + hashlib.sha256(b'{"page":1}').hexdigest() for r in recorded)
    assert recorded[0]['observed_at'] <= recorded[1]['observed_at']
    evidence.finish()
    verify_evidence(evidence.artifact_dir)


@pytest.mark.parametrize('method,status,content', [('GET', 302, None), ('POST', 307, b'{"page":1}')])
def test_a_redirect_retains_every_hop_and_its_request_body(tmp_path, method, status, content):
    """httpx rebuilds a redirect's request on an explicit stream, so its content was never read.

    SAM's extract download redirects; run 36219331804 (2026-09-26) died on RequestNotRead
    at the first retained redirect. A 307 re-sends the body, and both hops retain it.
    """
    evidence = CaptureEvidence(tmp_path, 'test')

    def respond(request):
        if request.url.host == 'native.test':
            return httpx.Response(status, headers={'Location': 'https://files.test/extract.zip'})
        return httpx.Response(200, stream=Chunks([b'PK', b'\x03\x04']))

    class Wire(httpx.BaseTransport):
        """Like httpx.HTTPTransport, and unlike MockTransport, it never reads the request."""

        def handle_request(self, request):
            return respond(request)

    transport = evidence.transport(Wire(), stage='native', max_bytes=1000)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        assert client.request(method, 'https://native.test/download', content=content).content == b'PK\x03\x04'
    recorded = captures(evidence)
    assert [(r['status_code'], r['requested_url']) for r in recorded] == [
        (status, 'https://native.test/download'), (200, 'https://files.test/extract.zip')]
    expected = None if content is None else 'sha256:' + hashlib.sha256(content).hexdigest()
    assert [r['request_body'] and r['request_body']['sha256'] for r in recorded] == [expected, expected]
    evidence.finish()
    verify_evidence(evidence.artifact_dir)


@pytest.mark.parametrize('parts,max_bytes,error', [([b'ab', b'cd'], 3, SourceEvidenceError),
                                                  ([b'aa-sec', b'ret-bb'], 100, CredentialRefusedError),
                                                  ([b'aa-s', b'e', b'c', b'r', b'et-bb'], 100, CredentialRefusedError)])
def test_stream_overflow_or_split_credential_never_retains_blob(tmp_path, parts, max_bytes, error):
    evidence = CaptureEvidence(tmp_path, 'test')
    evidence.credential = 'secret'
    transport = evidence.transport(httpx.MockTransport(lambda r: httpx.Response(200, stream=Chunks(parts))),
                                   stage='native', max_bytes=max_bytes)
    with httpx.Client(transport=transport) as client, pytest.raises(error):
        client.get('https://native.test/?api_key=secret')
    assert not captures(evidence)
    assert not [p for p in (evidence.artifact_dir / 'blobs').rglob('*') if p.is_file()]
    assert all(b'secret' not in p.read_bytes() for p in evidence.directory.rglob('*') if p.is_file())


def test_retained_file_that_differs_from_its_digest_never_seals(tmp_path):
    evidence = CaptureEvidence(tmp_path, 'test')
    body = b'exact native bytes'

    def retain(sha256):
        return evidence._retain_file(
            io.BytesIO(body), sha256=sha256, byte_size=len(body), stage='download',
            requested_url='https://native.test/data', resolved_url='https://native.test/data',
            observed_at='2026-09-25T00:00:00Z', status_code=200, content_type='application/octet-stream',
            content_encoding='identity', method='GET', request_body=None,
        )

    with pytest.raises(SourceEvidenceError, match='file bytes'):
        retain('sha256:' + '0' * 64)
    assert not captures(evidence)
    assert not [p for p in (evidence.artifact_dir / 'blobs').rglob('*') if p.is_file()]
    # The store verified the bytes; the evidence now refuses to claim a complete build.
    assert retain('sha256:' + hashlib.sha256(body).hexdigest())['byte_size'] == len(body)
    with pytest.raises(SourceEvidenceError, match='cannot seal as a complete build'):
        evidence.inputs()


def test_a_swallowed_retention_failure_still_finishes_the_run_failed(tmp_path):
    """A caller's broad ``except`` cannot turn a retention failure into one item's refusal."""
    evidence = CaptureEvidence(tmp_path, 'test')
    transport = evidence.transport(httpx.MockTransport(lambda r: httpx.Response(200, stream=Chunks([b'abcd']))),
                                   stage='native', max_bytes=3)
    with httpx.Client(transport=transport) as client:
        try:
            client.get('https://native.test/')
        except Exception:  # noqa: BLE001 — the pattern under test
            pass
    with pytest.raises(SourceEvidenceError, match='byte bound'):
        evidence.finish()
    outcome = json.loads((evidence.directory / 'run-outcome.json').read_bytes())
    assert outcome['outcome'] == 'failed' and outcome['error_type'] == 'SourceEvidenceError'
    with pytest.raises(SourceEvidenceError, match='completed-build'):
        verify_evidence(evidence.artifact_dir)


def test_a_rollup_that_swallows_a_retention_failure_publishes_no_generation(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)

    class Swallowing(RollupPipeline):
        name = 'test'
        output = 'a.parquet'
        retain_source_evidence = True

        def build(self, output_dir):
            assert self.source_evidence is not None
            monkeypatch.setattr(self.source_evidence.store, 'put_blob',
                                lambda *a, **kw: (_ for _ in ()).throw(OSError('disk full')))
            try:
                self.source_evidence.capture(_owner_capture(b'used'), stage='used')
            except Exception:  # noqa: BLE001 — a transform treating it as a source refusal
                pass
            path = output_dir / self.output
            pq.write_table(pa.table({'id': ['one']}), path)
            return path

    pipeline = Swallowing(output_dir=tmp_path)
    with pytest.raises(SourceEvidenceError, match='cannot seal as a complete build'):
        pipeline.run()
    assert not list((tmp_path / 'generations').glob('*'))
    evidence = pipeline.source_evidence
    assert evidence is not None
    assert json.loads((evidence.directory / 'run-outcome.json').read_bytes())['outcome'] == 'failed'


@pytest.mark.parametrize('kind', ['crs', 'gao', 'court'])
def test_native_shape_refusal_retains_evidence_before_parse(tmp_path, kind):
    evidence = CaptureEvidence(tmp_path, kind)
    raw = b'<!DOCTYPE html><html>not a native API response</html>'
    transport = httpx.MockTransport(lambda r: httpx.Response(200, content=raw, headers={'content-type': 'text/html'}))
    if kind == 'crs':
        source = CrsReportsReader(api_key='fixture-key', transport=transport, evidence=evidence)
    elif kind == 'gao':
        source = GaoReportsReader(transport=transport, evidence=evidence)
    else:
        source = CourtListenerReader(api_token="fixture-token", transport=transport, evidence=evidence)
    with pytest.raises(ValueError) as raised:
        list(source.iter_records())
    if kind == 'gao':
        [capture] = captures(evidence)
        assert capture['sha256'] == 'sha256:' + hashlib.sha256(raw).hexdigest()
        assert capture['byte_size'] == len(raw) and capture['body_retained'] is False
        assert capture['evidence_policy'] == 'hash_only' and 'blob_member' not in capture
        assert not (evidence.artifact_dir / 'blobs/sha256' / capture['sha256'][7:]).exists()
    else:
        assert any(payload(evidence, c) == raw for c in captures(evidence))
    evidence.finish(raised.value)
    with pytest.raises(SourceEvidenceError, match='completed-build'):
        verify_evidence(evidence.artifact_dir)


def test_usaspending_observation_and_old_schema_merge(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path, 'usaspending-recipients')
    old = [{'recipient_id': 'old-R', 'uei': 'OLD', 'duns': None, 'name': 'Old', 'recipient_level': 'R', 'total_award_amount': '4'},
           {'recipient_id': 'fresh-R', 'uei': 'FRESH', 'duns': None, 'name': 'Before', 'recipient_level': 'R', 'total_award_amount': '5'}]
    prior = tmp_path / '_usaspending_prior.parquet'
    pq.write_table(pa.Table.from_pylist(old, schema=pa.schema([(c, pa.string()) for c in usa.COLUMNS[:6]])), prior)
    body = {'results': [{'id': 'fresh-R', 'uei': 'FRESH', 'name': 'After', 'recipient_level': 'R', 'amount': 9.5}],
            'page_metadata': {'total': 1, 'next': None, 'hasNext': False}}
    raw = json.dumps(body).encode()
    real = usa._iter_recipient_rows
    monkeypatch.setattr(usa, '_iter_recipient_rows', lambda **kw: real(
        **kw, transport=httpx.MockTransport(lambda r: httpx.Response(200, content=raw, headers={'content-type': 'application/json'}))))
    output = usa.build_usaspending_recipients(tmp_path, evidence=evidence)
    from tests.government_fakes import literal_table
    rows = {r['recipient_id']: r for r in literal_table(output).to_pylist()}
    assert rows['old-R']['observed_at'] is None
    assert rows['old-R']['source_capture_sha256'] is None
    observed = rows['fresh-R']
    assert observed['name'] == 'After' and observed['total_award_amount'] == '9.5'
    # One retained copy per response: the tee's, which the owner's capture digest names.
    [capture] = captures(evidence)
    assert capture['stage'] == 'usaspending-response'
    assert observed['source_capture_sha256'] == capture['sha256']
    assert payload(evidence, capture) == raw
    # The row's time is the owner's completed read, never before the tee saw the request.
    assert datetime.fromisoformat(observed['observed_at']) >= datetime.fromisoformat(capture['observed_at'])
    # ...and the journal states that exact time for the same digest, so the row is reproducible from evidence.
    events = [json.loads(line) for line in (evidence.artifact_dir / 'journal.jsonl').read_text().splitlines()]
    [read] = [e for e in events if e['event'] == 'page-read']
    assert (read['sha256'], read['observed_at']) == (observed['source_capture_sha256'], observed['observed_at'])
    # A subsequent successful empty selection must preserve the existing row times.
    import shutil
    shutil.copyfile(output, prior)
    monkeypatch.setattr(usa, '_iter_recipient_rows', lambda **kw: iter(()))
    second = usa.build_usaspending_recipients(tmp_path)
    assert {r['recipient_id']: r for r in literal_table(second).to_pylist()} == rows


def test_usaspending_output_is_its_declared_schema_and_admits_a_generation(tmp_path, monkeypatch):
    from spicy_regs.data_dictionary import expected_schemas
    from spicy_regs.generations import verify_generation
    from spicy_regs.pipelines.rollups.usaspending_recipients import UsaSpendingRecipientsRollup

    from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS
    assert SUBJECT_SCHEMAS['usaspending_recipients'].names == [column for column, _ in expected_schemas()['usaspending_recipients']]
    assert {'observed_at', 'source_capture_sha256'} <= set(usa.COLUMNS) - set(SUBJECT_SCHEMAS['usaspending_recipients'].names)
    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    monkeypatch.setattr(usa.r2, 'download', lambda *a: False)
    raw = json.dumps({'results': [{'id': 'fresh-R', 'uei': 'FRESH', 'name': 'After', 'recipient_level': 'R',
                                   'amount': 9.5}],
                      'page_metadata': {'total': 1, 'next': None, 'hasNext': False}}).encode()
    real = usa._iter_recipient_rows
    monkeypatch.setattr(usa, '_iter_recipient_rows', lambda **kw: real(
        **kw, transport=httpx.MockTransport(lambda r: httpx.Response(
            200, content=raw, headers={'content-type': 'application/json'}))))
    pipeline = UsaSpendingRecipientsRollup(output_dir=tmp_path, skip_upload=True)
    pipeline.run()
    [generation] = list((tmp_path / 'generations').iterdir())
    artifact = verify_generation(generation)
    evidence = pipeline.source_evidence
    assert evidence is not None and evidence.artifact is not None
    assert {'role': 'source-evidence', **evidence.artifact.pin.as_dict()} in artifact.root['inputs']
    verify_evidence(evidence.artifact_dir)


def test_retention_failure_prevents_candidate_replacement(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path, 'usaspending-recipients')
    output = tmp_path / usa.OUTPUT
    output.write_bytes(b'prior output remains')
    monkeypatch.setattr(usa.r2, 'download', lambda *a: False)
    monkeypatch.setattr(evidence.store, 'put_blob', lambda *a, **kw: (_ for _ in ()).throw(OSError('disk full')))
    raw = b'{"results": [], "page_metadata": {"total": 0, "next": null, "hasNext": false}}'
    real = usa._iter_recipient_rows
    monkeypatch.setattr(usa, '_iter_recipient_rows', lambda **kw: real(
        **kw, transport=httpx.MockTransport(lambda r: httpx.Response(200, content=raw))))
    with pytest.raises(SourceEvidenceError):
        usa.build_usaspending_recipients(tmp_path, evidence=evidence)
    assert output.read_bytes() == b'prior output remains'


def test_sam_extract_keeps_complete_raw_file_before_registration_mapping(tmp_path, monkeypatch):
    from tests.test_sam_entities import _RAW_ENTITY
    evidence = CaptureEvidence(tmp_path, 'sam-entities')
    monkeypatch.setenv('SAM_API_KEY', 'fixture-key')
    native = dict(_RAW_ENTITY)
    body = json.dumps({'totalRecords': 1, 'entityData': [native]}).encode()
    transport = httpx.MockTransport(lambda r: httpx.Response(200, stream=Chunks([body[:20], body[20:]])))
    # Exercise the real owner and the transform-created evidence observer.
    original = evidence.transport
    monkeypatch.setattr(evidence, 'transport', lambda *a, **kw: original(transport, **kw))
    rows = list(sam._iter_sam_entities(mode='extract', registration_status='A', since_year=2026, until_year=2026,
                                     year_windows=True, max_records=None, evidence=evidence))
    assert rows == [native]
    [capture] = captures(evidence)
    assert payload(evidence, capture) == body
    assert 'registrationDate' in capture['requested_url']
    assert 'fixture-key' not in json.dumps(capture)
    assert any(e['event'] == 'selection' and e['since_year'] == 2026 for e in journal(evidence))



def test_sam_extract_journals_each_renewal_it_credits_toward_the_count(tmp_path, monkeypatch):
    """SAM counts a renewal's prior record while it stays Active (2007: WRERXDACNQ31); the journal names it."""
    from tests.test_sam_entities import _RAW_ENTITY
    evidence = CaptureEvidence(tmp_path, 'sam-entities')
    monkeypatch.setenv('SAM_API_KEY', 'fixture-key')

    def version(updated, expires):
        return {**_RAW_ENTITY, 'entityRegistration': {**_RAW_ENTITY['entityRegistration'], 'entityEFTIndicator': None,
                                                      'lastUpdateDate': updated, 'registrationExpirationDate': expires}}

    older, newer = version('2024-09-04', '2026-09-30'), version('2026-09-09', '2027-08-27')
    body = json.dumps({'totalRecords': 2, 'entityData': [older, newer]}).encode()
    original = evidence.transport
    monkeypatch.setattr(evidence, 'transport', lambda *a, **kw: original(
        httpx.MockTransport(lambda r: httpx.Response(200, content=body)), **kw))
    rows = list(sam._iter_sam_entities(mode='extract', registration_status='A', since_year=2026, until_year=2026,
                                     year_windows=True, max_records=None, evidence=evidence))
    assert rows == [newer]
    [credit] = [e for e in journal(evidence) if e['event'] == 'sam-superseded-credited']
    assert credit['stage'] == 'sam-extract:2026' and credit['uei'] == _RAW_ENTITY['entityRegistration']['ueiSAM']
    assert (credit['superseded_last_update'], credit['kept_last_update']) == ('2024-09-04', '2026-09-09')
    assert (credit['superseded_expiration'], credit['kept_expiration']) == ('2026-09-30', '2027-08-27')
    assert credit['kept_last_update'] < credit['trigger_day']

def test_invalid_content_encoding_keeps_complete_native_response(tmp_path):
    evidence = CaptureEvidence(tmp_path, 'test')
    raw = b'not actually gzip'
    transport = evidence.transport(httpx.MockTransport(lambda r: httpx.Response(
        200, headers={'content-encoding': 'gzip'}, stream=Chunks([raw]))), stage='native', max_bytes=100)
    with httpx.Client(transport=transport) as client, pytest.raises(httpx.DecodingError):
        client.get('https://native.test/')
    assert payload(evidence, captures(evidence)[0]) == raw


# --------------------------------------------------------------------------- #
# Evidence threads through each transform's own reader construction: the tee a
# transform builds sees the owner's real requests, and no credential is kept.
# --------------------------------------------------------------------------- #
KEY = 'fixture-key-0123456789'


def json_response(raw: bytes, status: int = 200) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(raw), headers={'content-type': 'application/json'})



def test_fcc_filings_retain_each_response_without_the_key(tmp_path, monkeypatch, tee):
    """The real ECFS reader's count and page requests are retained, so the generation replays from them."""
    from datetime import date

    fcc = importlib.import_module('spicy_regs.transforms.build_fcc_ecfs')
    monkeypatch.setenv('API_GOV', KEY)
    monkeypatch.setattr(fcc.r2, 'download', lambda *a: False)
    evidence = CaptureEvidence(tmp_path / 'audit', 'fcc-filings')
    filing = {'id_submission': 'f-1', 'date_submission': '2026-09-25T12:00:00.000Z',
              'date_received': '2026-09-25T00:00:00Z', 'express_comment': 1, 'proceedings': [{'name': '17-108'}]}

    def respond(request):
        rows = [filing] if int(request.url.params.get('offset', 0)) == 0 else []
        buckets = {'doc_count_error_upper_bound': 0, 'sum_other_doc_count': 0, 'buckets': [{'key': 1, 'doc_count': 1}]}
        response = json_response(json.dumps({'filing': rows, 'aggregations': {'express_comment': buckets}}).encode())
        response.headers['total'] = '1'
        return response

    tee(evidence, respond)
    output = fcc.build_fcc_filings(tmp_path, evidence=evidence, since=date(2026, 9, 25))
    assert [row['id_submission'] for row in pq.read_table(output).to_pylist()] == ['f-1']
    retained = captures(evidence)
    assert retained and {capture['stage'] for capture in retained} == {'fcc-filings-response'}
    assert all(capture['response_headers'] == [['total', '1']] for capture in retained)
    assert any(e['event'] == 'selection' and e['stage'] == 'fcc-filings' for e in journal(evidence))
    assert_no_secret(evidence, KEY)

def test_lobbying_retains_each_page_without_the_key(tmp_path, monkeypatch, tee):
    from datetime import date

    lda = importlib.import_module('spicy_regs.transforms.build_lobbying_filings')
    monkeypatch.setenv('LDA_API_KEY', KEY)
    monkeypatch.setattr(lda.r2, 'download', lambda *a: False)
    monkeypatch.setattr(lda, 'KEYED_INTERVAL_SECONDS', 0.0)
    evidence = CaptureEvidence(tmp_path / 'audit', 'lobbying-filings')
    raw = json.dumps({'count': 1, 'next': None, 'previous': None, 'results': [{'filing_uuid': 'f-1'}]}).encode()
    tee(evidence, lambda request: json_response(raw))
    output, *_ = lda.build_lobbying_filings(tmp_path, evidence=evidence, since=date(2026, 9, 1), until=date(2026, 9, 2))
    assert [row['filing_uuid'] for row in pq.read_table(output).to_pylist()] == ['f-1']
    [capture] = captures(evidence)
    assert capture['stage'] == 'lobbying-response' and payload(evidence, capture) == raw
    assert any(e['event'] == 'selection' and e['stage'] == 'lobbying' for e in journal(evidence))
    assert_no_secret(evidence, KEY)


def test_sam_partition_retains_each_window_page_without_the_key(tmp_path, monkeypatch, tee):
    from tests.test_sam_entities import _RAW_ENTITY

    evidence = CaptureEvidence(tmp_path, 'sam-entities')
    monkeypatch.setenv('SAM_API_KEY', KEY)
    body = json.dumps({'totalRecords': 1, 'entityData': [dict(_RAW_ENTITY)]}).encode()
    tee(evidence, lambda request: httpx.Response(200, stream=Chunks([body[:20], body[20:]]),
                                                 headers={'content-type': 'application/json'}))
    rows = list(sam._iter_sam_entities(mode='partition', registration_status='A', since_year=2026, until_year=2026,
                                       year_windows=False, max_records=None, evidence=evidence))
    assert rows == [dict(_RAW_ENTITY)]
    retained = captures(evidence)
    assert retained and all(c['stage'] == 'sam-page' and payload(evidence, c) == body for c in retained)
    assert any(e['event'] == 'selection' and e['mode'] == 'partition' for e in journal(evidence))
    assert_no_secret(evidence, KEY)


def test_amendments_retain_listing_pages_without_the_key(tmp_path, monkeypatch, tee):
    from dataclasses import replace
    from datetime import date

    amendments = importlib.import_module('spicy_regs.transforms.build_amendments')
    monkeypatch.setenv('API_GOV', KEY)
    monkeypatch.setenv('BILL_FAMILY_CONGRESSES', '119')
    monkeypatch.setattr(amendments, 'BUDGET', replace(amendments.BUDGET, min_request_interval_seconds=0.0))
    evidence = CaptureEvidence(tmp_path / 'audit', 'amendments')
    raw = json.dumps({'amendments': [], 'pagination': {'count': 0}}).encode()
    tee(evidence, lambda request: json_response(raw))
    amendments.build_amendments(tmp_path, since=date(2026, 9, 1), until=date(2026, 9, 2),
                                download_prior=lambda *a: False, evidence=evidence)
    assert stages(evidence) == {'amendment-response'}
    assert all(payload(evidence, c) == raw for c in captures(evidence))
    assert any(e['event'] == 'amendment-selection' for e in journal(evidence))
    assert_no_secret(evidence, KEY)


def test_print_citations_retain_discovery_pages_without_the_key(tmp_path, monkeypatch, tee):
    from dataclasses import replace

    prints = importlib.import_module('spicy_regs.transforms.build_print_citations')
    monkeypatch.setenv('API_GOV', KEY)
    monkeypatch.setattr(prints, 'DISCOVERY_BUDGET', replace(prints.DISCOVERY_BUDGET, min_request_interval_seconds=0.0))
    monkeypatch.setattr(prints, 'ROSTER_BUDGET', replace(prints.ROSTER_BUDGET, min_request_interval_seconds=0.0))
    evidence = CaptureEvidence(tmp_path / 'audit', 'print-citations')
    raw = json.dumps({'count': 0, 'packages': [], 'nextPage': None}).encode()
    tee(evidence, lambda request: json_response(raw))
    prints.build_print_citations(tmp_path, download_prior=lambda *a: False, evidence=evidence)
    assert 'print-discovery' in stages(evidence)
    assert all(payload(evidence, c) == raw for c in captures(evidence))
    assert_no_secret(evidence, KEY)


def test_bill_subjects_api_and_bulk_readers_retain_their_responses_without_the_key(tmp_path, tee):
    import zipfile
    from pathlib import Path

    from spicy_regs.sources.bill_subjects import BillSubjectsFetcher
    from spicy_regs.transforms.enrich_bill_subjects import _folder_reader

    subjects = json.dumps({'subjects': {'policyArea': {'name': 'Taxation'},
                                        'legislativeSubjects': [{'name': 'Income tax'}]},
                           'pagination': {'count': 1}}).encode()
    buffer = io.BytesIO()
    status = Path(__file__).parent / 'fixtures' / 'govinfo_bills' / 'status-119hr6028.xml'
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr('BILLSTATUS-119hr6028.xml', status.read_bytes())
    zipped = buffer.getvalue()
    evidence = CaptureEvidence(tmp_path, 'bill-subjects')
    tee(evidence, lambda request: json_response(subjects) if request.url.path.endswith('/subjects') else httpx.Response(
        200, stream=httpx.ByteStream(zipped), headers={'content-type': 'application/zip'}))
    with BillSubjectsFetcher(api_key=KEY, evidence=evidence) as fetcher:
        answer = fetcher.subjects_for('119', 'hr', '6028')
    assert answer is not None and answer.policy_area == 'Taxation'
    with _folder_reader(None, evidence) as read:
        [member] = read(119, 'hr').members
    assert member.identity is not None and member.identity.number == 6028
    by_stage = {c['stage']: payload(evidence, c) for c in captures(evidence)}
    assert by_stage == {'bill-subject-response': subjects, 'subject-bulk': zipped}
    assert_no_secret(evidence, KEY)


# --------------------------------------------------------------------------- #
# Every rollup that reads a publisher keeps what it read, or says here why not.
# --------------------------------------------------------------------------- #
_UNWIRED = "not wired yet: T18 queue (open-work-investigation-2026-09-26/evidence-partials.md)"
EVIDENCE_EXEMPT = {
    "DocketsFamily": "republishes the ETL's own working copy; the ETL's Mirrulations reads are the source reads",
    "DocumentsFamily": "republishes the ETL's own working copy; the ETL's Mirrulations reads are the source reads",
    "DocketAttributesFamily": "republishes the ETL's own working copy; the ETL's Mirrulations reads are the source reads",
    "DocumentAttributesFamily": "republishes the ETL's own working copy; the ETL's Mirrulations reads are the source reads",
    "CommentAttributesFamily": "republishes the working copy the comment re-read and the ETL's Mirrulations reads write",
    "CfrSectionsRollup": _UNWIRED,
    "FecCommitteesRollup": _UNWIRED + "; its captures live only as an expiring workflow artifact",
    "FecSourceCatalogRollup": _UNWIRED,
    "PressReleasesRollup": _UNWIRED,
    "SenateExpendituresRollup": _UNWIRED,
}


def _ingest_rollups() -> dict[str, type[RollupPipeline]]:
    """Concrete rollups with no base-table inputs: each reads an outside source."""
    import pkgutil

    import spicy_regs.pipelines.rollups as package

    found: dict[str, type[RollupPipeline]] = {}
    for info in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(f"{package.__name__}.{info.name}")
        for cls in vars(module).values():
            if (isinstance(cls, type) and issubclass(cls, RollupPipeline) and cls.__module__ == module.__name__
                    and "name" in vars(cls) and not cls.inputs):
                found[cls.__name__] = cls
    return found


def test_every_ingest_rollup_retains_its_source_evidence_or_names_why_not():
    """FCC filings published unevidenced generations for days because nothing held the rollups to this."""
    rollups = _ingest_rollups()
    unexplained = sorted(name for name, cls in rollups.items()
                         if not cls.retain_source_evidence and name not in EVIDENCE_EXEMPT)
    assert unexplained == [], f"retain source evidence, or add a reason to EVIDENCE_EXEMPT: {unexplained}"
    stale = sorted(name for name in EVIDENCE_EXEMPT
                   if name not in rollups or rollups[name].retain_source_evidence)
    assert stale == [], f"remove these from EVIDENCE_EXEMPT: {stale}"
