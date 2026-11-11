"""Ordinary court runners must pass the shared receipt and native schema hooks; the bulk ones publish none."""
import importlib

import pytest

from spicy_regs.court_receipts import write_court_rows
from spicy_regs.generations import verify_generation

CASES = [
    ('court_opinion_clusters', 'CourtOpinionClustersRollup', {'court_opinion_clusters': {'cluster_id': '1'}}),
    ('courtlistener', 'CourtListenerRollup', {'court_dockets': {'cl_docket_id': '1', 'parties_json': '[]'}}),
]


@pytest.mark.parametrize('module_name,class_name,records', CASES)
def test_runner_admits_receipts_and_typed_subjects(tmp_path, monkeypatch, module_name, class_name, records):
    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    module = importlib.import_module('spicy_regs.pipelines.rollups.' + module_name)
    cls = getattr(module, class_name)

    def build(self, output):
        paths = tuple(write_court_rows(name, [row], output, generation_id='local-runner-build',
            witnesses=[{'source_id': 'fixture', 'source_uri': None, 'sha256': 'a' * 64,
                        'locator': None, 'body_version': None}]) for name, row in records.items())
        return paths if len(paths) > 1 else paths[0]

    monkeypatch.setattr(cls, 'build', build)
    cls(output_dir=tmp_path, skip_upload=True).run()
    generation, = (tmp_path / 'generations').iterdir()
    artifact = verify_generation(generation)
    assert artifact.root['spec']['etlReceipts']['generationId'] == 'local-runner-build'
    assert set(artifact.root['spec']['tables']) == {name + '.parquet' for name in records}
    assert (generation / 'etl_receipts.parquet').is_file()

    from spicy_regs.local_data import local_selection
    selected = local_selection(tmp_path)
    assert set(selected.files) == set(records)
    for name in records:
        assert selected.paths(name) == (generation / (name + '.parquet'),)
        assert selected.native[name].receipts == generation / 'etl_receipts.parquet'
        assert selected.native[name].generation_id == 'local-runner-build'


@pytest.mark.parametrize('module_name,class_name', [('court_citations', 'CourtCitationsRollup'),
                                                    ('court_opinions', 'CourtOpinionsRollup')])
def test_the_bulk_runners_admit_their_tables_with_no_receipts(tmp_path, monkeypatch, module_name, class_name):
    """Their policies are marked retired (ss-vse3): a plain generation, evidence beside it, no receipt member."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.court_subjects import SUBJECT_SCHEMAS
    from spicy_regs.subject_catalog import retired_receipt_datasets

    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    monkeypatch.setenv('SPICYGOV_RAW_ROOT', str(tmp_path / 'raw'))
    module = importlib.import_module('spicy_regs.pipelines.rollups.' + module_name)
    cls = getattr(module, class_name)
    names = [key.removesuffix('.parquet') for key in (cls.outputs or (cls.output,))]
    assert set(names) <= retired_receipt_datasets()

    def build(self, output):
        assert self.source_evidence is not None
        paths = []
        for name in names:
            path = output / (name + '.parquet')
            pq.write_table(pa.Table.from_pylist([], schema=SUBJECT_SCHEMAS[name]), path)
            paths.append(path)
        return tuple(paths) if len(paths) > 1 else paths[0]

    monkeypatch.setattr(cls, 'build', build)
    cls(output_dir=tmp_path, skip_upload=True).run()
    generation, = (tmp_path / 'generations').iterdir()
    artifact = verify_generation(generation)
    assert 'etlReceipts' not in artifact.root['spec']
    assert set(artifact.root['spec']['tables']) == {name + '.parquet' for name in names}
    assert not (generation / 'etl_receipts.parquet').exists()
    assert [item['role'] for item in artifact.root['inputs']] == ['source-evidence']
