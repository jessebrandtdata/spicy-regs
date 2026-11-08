"""Explicit target retries stay bounded and retained successes bypass acquisition."""
from typing import Any
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from tests.government_fakes import literal_table
import pytest

from spicy_docs.sources.gao.files import gao_report_index_locator
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.transforms.build_gao_reports import _SCHEMA
from spicy_regs.transforms.build_gao_target import append_missing_target,target_index
from spicy_regs.citation_resolution import resolve_citations
import duckdb

PRODUCT='gao-17-317'
BODY=b'<title>High-Risk Series</title><a href="https://files.gao.gov/assets/gao-17-317.pdf">PDF</a><a href="https://www.gao.gov/products/gao-17-317">Product</a>'


def retained():
    url=gao_report_index_locator(PRODUCT)
    return CapturedBodyResponse(url,url,200,'text/html','2026-09-27T00:00:00Z',BODY)


def test_refusal_explicit_retry_and_retained_replay():
    calls=[]
    def refused(request):
        calls.append(request.url)
        return httpx.Response(403,stream=httpx.ByteStream(b'AccessDenied'))
    with pytest.raises(CredentialRefusedError):
        target_index(PRODUCT,transport=httpx.MockTransport(refused))
    assert len(calls)==1
    def success(request):
        calls.append(request.url)
        return httpx.Response(200,headers={'content-type':'text/html'},stream=httpx.ByteStream(BODY))
    index,capture=target_index(PRODUCT,transport=httpx.MockTransport(success))
    assert len(calls)==2 and index.product_id==PRODUCT
    again,_=target_index(PRODUCT,retained=capture,transport=httpx.MockTransport(refused))
    assert again==index and len(calls)==2


def test_bad_retained_identity_refuses_without_retry():
    bad=retained()
    bad=CapturedBodyResponse(bad.requested_url,bad.resolved_url,200,'text/html',bad.observed_at,b'<title>Other</title>')
    def forbidden(request):
        raise AssertionError('Retained input must not trigger acquisition')
    with pytest.raises(ValueError):
        target_index(PRODUCT,retained=bad,transport=httpx.MockTransport(forbidden))


def test_candidate_preserves_all_prior_cells_leaves_date_unknown_and_resolves(tmp_path):
    prior=tmp_path/'prior.parquet'
    out=tmp_path/'candidate.parquet'
    row=dict.fromkeys(_SCHEMA.names)
    row.update(report_id='gao-26-1',title='Existing',published_date='2026-01-02',topics_json='[]')
    pq.write_table(pa.Table.from_pylist([row],schema=_SCHEMA),prior)
    index,_=target_index(PRODUCT,retained=retained())
    report=append_missing_target(prior,out,index)
    assert report['prior_cells_equal'] and report['candidate_rows']==2
    added=literal_table(out, "gao_reports").to_pylist()[1]
    assert added['published_date'] is None and added['topics_json'] is None
    assert added['source']=='gao_repair'
    c: Any=duckdb.connect()
    c.read_parquet(str(out)).create_view('gao_reports')
    result=resolve_citations(c,[{'cite_kind':'gao_product_id','target_key':'GAO-17-317'}],
                             {'gao_reports':{'artifact_digest':'synthetic-unit-test-pin'}})
    assert result['occurrences'][0]['target_status']=='found'
    with pytest.raises(ValueError,match='already exists'):
        append_missing_target(out,tmp_path/'overwrite.parquet',index)


@pytest.mark.parametrize("product_page", [False, True])
@pytest.mark.parametrize("with_decisions", [False, True])
def test_complete_generation_candidate_uses_prior_pin_without_publication(tmp_path,monkeypatch,product_page,with_decisions):
    from spicy_regs.generations import build_generation,verify_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.build_gao_target import prepare_target_generation
    from tests.generation_fakes import Store
    import json
    prior=tmp_path/'gao_reports.parquet'
    row=dict.fromkeys(_SCHEMA.names)
    row.update(report_id='gao-26-1',title='Existing')
    pq.write_table(pa.Table.from_pylist([row],schema=_SCHEMA),prior)
    directory=tmp_path/'prior-generation'
    from spicy_regs.transforms.government_receipts import migrate_outputs, generation_receipt_args, _legacy_schema
    outputs = [prior]
    if with_decisions:
        decisions = tmp_path/'gao_decisions.parquet'
        pq.write_table(pa.Table.from_pylist([{'decision_number':'B-1','url':'https://gao.test/b1',
            'released_date':'2025-01-02','decided_date':'2025-01-01','decision_status':'Denied.',
            'outcome':'denied','source':'listing','b_numbers_json':'["B-1"]'}], schema=_legacy_schema('gao_decisions')), decisions)
        outputs.append(decisions)
    migrate_outputs(tuple(outputs), generation_id='prior')
    before = pq.read_table(outputs[-1]) if with_decisions else None
    build_generation(directory,family='gao-reports',files=outputs,expected_keys=[p.name for p in outputs],
                     **generation_receipt_args(tuple(outputs)))
    store=Store()
    snapshot=publication.publish_generation(directory,client=store,bucket='test',prior_index=publication.empty_index())
    raw=(directory/'artifact.json').read_bytes()
    monkeypatch.setattr(publication,'load_family_root',lambda *_:(raw,json.loads(raw)))
    def fetch_member(base, member, target, *args):
        target.write_bytes(store.objects[member.path])
        return True
    monkeypatch.setattr(publication,'fetch_member',fetch_member)
    from pathlib import Path
    page = (Path(__file__).parent/'fixtures/gao_target/product-page.zip').read_bytes() if product_page else None
    report=prepare_target_generation(tmp_path/'target',product_id=None if product_page else PRODUCT,prior_file=prior,
        prior_index=snapshot,public_url='https://test.invalid',
        retained_index=None if product_page else retained(),
        retained_product_pages={PRODUCT: page} if page is not None else None)
    artifact=verify_generation(tmp_path/'target/generation')
    assert report['status']=='verified_candidate_not_published'
    # The family's own prior is its prior-generation input, never a parent.
    prior_digest=snapshot['families']['gao-reports']['artifactDigest']
    assert 'parents' not in artifact.root['spec']
    assert [i['artifactDigest'] for i in artifact.root['inputs'] if i['role']=='prior-generation']==[prior_digest]
    assert report['candidate_rows']==2
    if product_page:
        assert page is not None
        from hashlib import sha256
        from spicy_regs.source_evidence import verify_evidence
        evidence_dir = Path(report['evidence_directory'])
        admitted = verify_evidence(evidence_dir)
        publication.publish_evidence(store, 'test', evidence_dir, admitted)
        events = [json.loads(line) for line in (evidence_dir/'journal.jsonl').read_text().splitlines()]
        page_event = next(event for event in events if event.get('stage') == 'gao-retained-product-page')
        assert page_event['evidence_policy'] == 'hash_only' and page_event['body_retained'] is False
        assert page_event['sha256'] == 'sha256:' + sha256(page).hexdigest()
        assert page_event['byte_size'] == len(page) and 'blob_member' not in page_event
        assert 'source-evidence/blobs/sha256/' + sha256(page).hexdigest() not in store.objects
        assert not any(page == body for body in store.objects.values())
    if with_decisions:
        assert pq.read_table(tmp_path/'target/generation/gao_decisions.parquet').equals(before)


def test_retained_product_page_qualifies_exact_heading_and_explicit_day():
    from pathlib import Path
    from spicy_docs.sources.gao.product_metadata import product_page_metadata
    raw=(Path(__file__).parent/'fixtures/gao_target/product-page.zip').read_bytes()
    meta=product_page_metadata(raw,PRODUCT)
    assert meta.title=='High-Risk Series: Progress on Many High-Risk Areas, While Substantial Efforts Needed on Others'
    assert meta.published_date=='2017-02-15'
    assert meta.pdf_url=='https://files.gao.gov/assets/gao-17-317.pdf'
    with pytest.raises(ValueError,match='different product'):
        product_page_metadata(raw,'gao-17-999')


def _page(body: bytes) -> bytes:
    from spicy_docs.sources.gao.native import iter_gao_product_pages
    from spicy_docs.sources.zyte import ZyteHttpResponse
    url='https://www.gao.gov/products/'+PRODUCT
    pages=list(iter_gao_product_pages(lambda _:ZyteHttpResponse(url,url,200,'text/html',body),
                                     query_scope={'productIds':[PRODUCT]}))
    return pages[0].response_bytes


def test_a_page_without_exactly_one_full_report_link_is_refused():
    """SpicyDocs 0.50.1 refuses such a page. The label ``gao-qualified-page-heading-publication-block/1`` stays:
    a page it still reads yields the heading and day 0.50.0 read, and every retained page links one Full Report."""
    from pathlib import Path
    from zipfile import ZipFile
    from spicy_docs.sources.gao.product_metadata import product_page_metadata
    with ZipFile(Path(__file__).parent/'fixtures/gao_target/product-page.zip') as archive:
        body=archive.read('product.html')
    label=b'field__item">Full Report</div>'
    assert body.count(label)==1
    assert product_page_metadata(_page(body),PRODUCT).title
    # None, then two: the Highlights PDF relabelled as a second Full Report.
    for changed in (body.replace(label,b'field__item">Report</div>'),
                    body.replace(b'field__item">Highlights Page</div>',label)):
        assert changed!=body
        with pytest.raises(ValueError,match='exactly one Full Report'):
            product_page_metadata(_page(changed),PRODUCT)


def test_month_only_heading_block_does_not_infer_day():
    from pathlib import Path
    from zipfile import ZipFile
    from spicy_docs.sources.gao.product_metadata import product_page_metadata
    with ZipFile(Path(__file__).parent/'fixtures/gao_target/product-page.zip') as archive:
        body=archive.read('product.html').replace(b'Published: Feb 15, 2017.',b'Published: February 2017.')
    assert product_page_metadata(_page(body),PRODUCT).published_date is None


#: GAO's Bid Protest Annual Reports, FY2021 and FY2025 (fixtures README): pages with no topic and an unlabeled date.
ANNUAL_REPORTS = {"gao-22-900379": ("GAO Bid Protest Annual Report to Congress for Fiscal Year 2021", "2021-11-16"),
                  "gao-26-900695": ("GAO Bid Protest Annual Report to Congress for Fiscal Year 2025", "2025-12-12")}


def test_one_candidate_appends_several_retained_pages_and_carries_the_familys_other_table(tmp_path, monkeypatch):
    """The gao-reports family holds gao_decisions beside gao_reports; a repair candidate carries it byte for byte.

    Several retained product pages append in one candidate over the published prior, each under spicy-docs'
    product-page rule, so the five annual reports are one verified local generation, not five chained ones.
    """
    from pathlib import Path
    import json
    from spicy_docs.sources.gao.product_metadata import PRODUCT_PAGE_METADATA_RULE
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.build_gao_target import prepare_target_generation
    from tests.generation_fakes import Store

    prior = tmp_path / 'gao_reports.parquet'
    row = dict.fromkeys(_SCHEMA.names)
    row.update(report_id='gao-26-1', title='Existing')
    pq.write_table(pa.Table.from_pylist([row], schema=_SCHEMA), prior)
    decisions = tmp_path / 'gao_decisions.parquet'
    pq.write_table(pa.table({'decision_number': ['B-1.1'], 'url': ['https://www.gao.gov/products/b-1.1']}), decisions)
    directory = tmp_path / 'prior-generation'
    from spicy_regs.transforms.government_receipts import migrate_outputs, generation_receipt_args

    migrate_outputs((prior, decisions), generation_id="prior-multiple-targets")
    held_decisions = pq.read_table(decisions)
    build_generation(directory, family='gao-reports', files=[prior, decisions],
                     expected_keys=[prior.name, decisions.name], **generation_receipt_args((prior, decisions)))
    store = Store()
    snapshot = publication.publish_generation(directory, client=store, bucket='test',
                                              prior_index=publication.empty_index())
    def fetch_member(base, member, target, *args):
        target.write_bytes(store.objects[member.path])
        return True
    monkeypatch.setattr(publication, 'fetch_member', fetch_member)
    raw = (directory / 'artifact.json').read_bytes()
    monkeypatch.setattr(publication, 'load_family_root', lambda *_: (raw, json.loads(raw)))
    pages = {product: (Path(__file__).parent / f'fixtures/gao_target/{product}.zip').read_bytes()
             for product in ANNUAL_REPORTS}

    report = prepare_target_generation(tmp_path / 'target', prior_file=prior, prior_index=snapshot,
                                       public_url='https://test.invalid', retained_product_pages=pages,
                                       carried_files={decisions.name: decisions})
    assert report['status'] == 'verified_candidate_not_published'
    assert report['added_report_ids'] == sorted(ANNUAL_REPORTS) and report['candidate_rows'] == 3
    generation = tmp_path / 'target/generation'
    artifact = verify_generation(generation)
    from spicy_regs.transforms.government_receipts import internal_prior

    processing = internal_prior("gao_reports", generation / "gao_reports.parquet",
        receipt_path=generation / "etl_receipts.parquet", generation_id=artifact.root["spec"]["etlReceipts"]["generationId"])
    added = {r['report_id']: r for r in pq.read_table(processing).to_pylist()[1:]}
    assert {product: (r['title'], r['published_date'], r['topics_json'], r['source'])
            for product, r in added.items()} == {
        product: (title, day, None, 'gao_repair') for product, (title, day) in ANNUAL_REPORTS.items()}
    assert pq.read_table(generation / 'gao_decisions.parquet').equals(held_decisions)
    events = [json.loads(line) for line in Path(report['evidence_directory'], 'journal.jsonl').read_text().splitlines()]
    replays = [e for e in events if e['event'] == 'retained-product-page-replay']
    assert {e['product_id'] for e in replays} == set(ANNUAL_REPORTS)
    assert {e['rule'] for e in replays} == {PRODUCT_PAGE_METADATA_RULE}


def test_retained_page_failure_keeps_exception_body_and_message_out_of_evidence(tmp_path, monkeypatch):
    import json
    from spicy_docs.sources.gao import product_metadata
    from spicy_regs.sources import publication
    from spicy_regs.transforms import build_gao_target as target
    from spicy_docs.sources.gao.product_details import GaoProductDetailsError

    contact = b'Named official: (202) 512-1234 official@gao.gov'
    capture = CapturedBodyResponse('https://www.gao.gov/products/'+PRODUCT,
        'https://www.gao.gov/products/'+PRODUCT, 200, 'text/html', '2026-10-05T00:00:00Z', contact)
    error = GaoProductDetailsError('Named official: official@gao.gov')
    setattr(error, 'capture', capture)
    def refused(*_):
        raise error
    monkeypatch.setattr(product_metadata, 'product_page_metadata', refused)
    monkeypatch.setattr(publication, 'table_owner', lambda *_: ('gao-reports', {'tables': ['gao_reports.parquet']}))
    monkeypatch.setattr(target, '_verified_member', lambda *_: None)
    from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
    monkeypatch.setattr(CaptureEvidence, 'inherit', lambda *_args, **_kwargs: None)
    with pytest.raises(GaoProductDetailsError):
        target.prepare_target_generation(tmp_path/'target', prior_file=tmp_path/'prior', prior_index={},
            public_url='https://test.invalid', retained_product_pages={PRODUCT: contact})
    journal = next((tmp_path/'target').rglob('journal.jsonl'))
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    assert 'official@gao.gov' not in json.dumps(events)
    assert not any(contact in path.read_bytes() for path in journal.parent.rglob('*') if path.is_file())
    from spicy_regs.source_evidence import SourceEvidenceError
    with pytest.raises(SourceEvidenceError, match='Only completed-build evidence'):
        verify_evidence(journal.parent)
    from rulespec_artifacts import admit_artifact, LocalMemberSource
    admit_artifact(LocalMemberSource(journal.parent))
