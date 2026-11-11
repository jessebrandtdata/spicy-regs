"""Native admission preserves the Register's two docket-link statements."""
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup
from spicy_regs.transforms.regulations_receipts import policy, write_records


def test_links_are_a_plain_rollup_since_their_receipts_retired():
    """ss-vse3 step 3: rebuilt whole from the pinned Register table, no receipts. The receipt writer tests below hold
    the regulations writer's link rules for the generations published before, which its archive still holds."""
    from spicy_regs import subject_catalog
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    assert issubclass(FrDocketLinksRollup, RollupPipeline) and not issubclass(FrDocketLinksRollup, SubjectReceiptRollup)
    assert "fr_docket_links" in subject_catalog.retired_receipt_datasets()


def test_nullable_ordinal_keeps_document_identity_required_and_refusals_retained(tmp_path):
    good = {'document_number': '2026-00002', 'publication_date': '2026-10-01',
            'docket_source_ordinal': None, 'docket_id': 'B', 'link_source': 'regulations_dot_gov_info'}
    rows = [good, {**good, 'document_number': None}, {**good, 'publication_date': None},
            {**good, 'docket_id': None}, {**good, 'undeclared_source_field': 'must refuse'}]
    contexts = [ReceiptContext('g', str(i), 'new-link-regression', [{'source_id': 'retained-link',
                'source_uri': 'https://example.gov/links', 'sha256': 'a' * 64,
                'locator': f'/rows/{i}', 'body_version': None}]) for i in range(len(rows))]
    subject, receipt = write_records('fr_docket_links', zip(rows, contexts), tmp_path / 'written')
    assert pq.read_table(subject)['document_number'].to_pylist() == ['2026-00002']
    observations = pq.read_table(receipt).to_pylist()
    assert [row['outcome'] for row in observations] == ['accepted', 'refused', 'refused', 'refused', 'refused']
    assert 'undeclared_source_field' in observations[-1]['processing_json']
    from spicy_regs.subject_catalog import descriptor_of

    declared = json.loads((Path(__file__).parents[1] /
                           'src/spicy_regs/etl_policies/fr_docket_links.json').read_text())
    assert descriptor_of(declared) == policy('fr_docket_links').descriptor()


def test_same_exact_link_identity_refuses_ambiguous_receipt_join(tmp_path):
    row = {'document_number': '2026-00003', 'publication_date': '2026-10-01',
           'docket_source_ordinal': None, 'docket_id': 'B', 'link_source': 'regulations_dot_gov_info'}
    contexts = [ReceiptContext('g', str(i), 'new-link-collision', [{'source_id': 'held',
                'source_uri': 'https://example.gov/links', 'sha256': 'b' * 64,
                'locator': f'/rows/{i}', 'body_version': None}]) for i in range(2)]
    with pytest.raises(ValueError, match='Duplicate or ambiguous receipt join'):
        write_records('fr_docket_links', zip([row, row], contexts), tmp_path / 'duplicate')
