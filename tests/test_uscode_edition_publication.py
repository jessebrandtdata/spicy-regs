"""Small native fixtures exercise additive packaging, never original archive readers.

The live prior still carries the receipts of the generations before retirement, so ``setup`` builds its prior with
receipts under a catalog that has not retired them yet; the addition then leaves them behind.
"""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import generations, subject_catalog, uscode_edition_publication as addition
from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, write_dataset
from spicy_regs.parquet_rows import write_rows
from spicy_regs.source_evidence import JOURNAL, is_original
from spicy_regs.sources import publication
from spicy_regs.sources.uscode_editions import POLICIES


def digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def native(root, prefix, count=1, *, receipts=False):
    """A prior's tables (with the receipts the live one still has) or an accepted candidate's, with its original."""
    root.mkdir()
    paths, shards, outputs = [], [], {}
    for name, policy in POLICIES.items():
        records = []
        for i in range(count):
            row = dict.fromkeys(policy.subject_schema.names)
            row[policy.identity_fields[0]] = prefix + "-" + str(i)
            row["edition_id"] = prefix + "-" + str(i)
            if "body_text" in row:
                row["body_text"] = "" if i % 2 else "α\nbody"
            if "source_observation_json" in row:
                row["source_observation_json"] = '{"nested":[null,"",{"x":1}]}'
            context = ReceiptContext(prefix + "-original-generation", prefix + "-" + name + "-" + str(i),
                                     "fixture", [{"source_id": "fixture", "source_uri": "https://example.test/",
                                                   "sha256": "sha256:" + "a" * 64, "locator": str(i)}],
                                     {"nested": [None, "", {"value": 1}]})
            records.append((row, context))
        if receipts:
            subject, shard = write_dataset(records, root / name, policy, validation_backend="reference")
            assert subject is not None
            shards.append(shard)
        else:
            (root / name).mkdir()
            subject = write_rows((row for row, _ in records), root / name / (name + ".parquet"), policy.subject_schema)
        paths.append(subject)
        outputs[name] = {"path": str(subject.relative_to(root)), "rows": count, "sha256": digest(subject)}
    if receipts:
        combine_receipts(shards, root / "etl_receipts.parquet")
    original = root / "original.zip"
    original.write_bytes(b"publisher bytes of " + prefix.encode())
    source = {"kind": "release-point-corpus", "release_point": "119-138", "sha256": digest(original)}
    selection = root / "selection.json"
    selection.write_text(json.dumps({"sources": [{**source, "path": str(original)}]}))
    build = root / "rebuild.json"
    build.write_text(json.dumps({"status": "source-only-built", "generation": prefix + "-original-generation",
                                 "outputs": outputs, "baseline_inputs": [],
                                 "inputs": [{**source, "path": str(original)}]}))
    audit = root / "audit.json"
    audit.write_text(json.dumps({"status": "verified-complete-selected-fixture", "verdict": "APPROVE",
        "generation": prefix + "-original-generation", "rows": {name: count for name in POLICIES},
        "selection_sha256": digest(selection),
        "evidence_sha256": {str(p): digest(p) for p in [*paths, selection, build]}}))
    return paths, {"path": str(audit), "sha256": digest(audit), "bytes": audit.stat().st_size}


def entry(directory, published_at="2026-10-13T00:00:00Z"):
    """The index entry a publication of ``directory`` would write, receipts included when it has them."""
    root = json.loads((directory / "artifact.json").read_text())
    spec = root["spec"]
    value = {
        "logicalId": root["logicalId"], "artifactDigest": root["artifactDigest"],
        "prefix": "generations/uscode-editions/" + root["artifactDigest"].removeprefix("sha256:"),
        "publishedAt": published_at,
        "tables": {k: {**v, "byteSize": (directory / k).stat().st_size, "sha256": digest(directory / k)}
                   for k, v in spec["tables"].items()},
    }
    if "etlReceipts" in spec:
        value["etlReceipts"] = {**{k: v for k, v in spec["etlReceipts"].items() if k != "policies"},
                                "sha256": digest(directory / "etl_receipts.parquet"),
                                "byteSize": (directory / "etl_receipts.parquet").stat().st_size,
                                "datasets": list(POLICIES)}
    snapshot = publication.empty_index()
    snapshot["families"][addition.FAMILY] = value
    return snapshot, root["artifactDigest"]


def setup(tmp_path, *, old_count=1, new_prefix="new", new_count=1):
    old_source = tmp_path / "old-input"
    with pytest.MonkeyPatch.context() as before_retirement:
        before_retirement.setattr(subject_catalog, "retired_receipt_datasets", frozenset)
        files, _ = native(old_source, "old", old_count, receipts=True)
        prior = tmp_path / "prior"
        generations.build_generation(prior, family=addition.FAMILY, files=files,
            expected_keys=[p.name for p in files], receipt_path=old_source / "etl_receipts.parquet",
            receipt_policies=list(POLICIES.values()), receipt_generation_id="old-original-generation")
    snapshot, expected_prior = entry(prior)
    candidate = tmp_path / "candidate"
    _, audit = native(candidate, new_prefix, new_count)
    return dict(prior_directory=prior, candidate_directory=candidate, audit_pin=audit,
                read_snapshot=snapshot, expected_prior=expected_prior, output=tmp_path / "addition")


def test_append_preserves_order_and_values_without_copy_and_leaves_retired_receipts(tmp_path, monkeypatch):
    args = setup(tmp_path, old_count=3, new_count=2)
    before = {p: (p.stat().st_ino, digest(p)) for root in [args['prior_directory'], args['candidate_directory']]
              for p in root.rglob('*.parquet')}
    captured = {}
    original = generations.build_generation

    def build(directory, **kwargs):
        captured.update({p.name: p.stat().st_ino for p in kwargs['files']})
        assert kwargs['owned_output_root'] == args['output'] and kwargs.get('receipt_path') is None
        return original(directory, **kwargs)

    monkeypatch.setattr(generations, 'build_generation', build)
    result = addition.prepare_addition(**args)
    target = Path(result['generation'])
    artifact = generations.verify_generation(target)
    assert artifact.root['spec']['readSnapshot'] == args['read_snapshot']
    assert artifact.root['spec']['publicationStatus'] == 'complete-family'
    assert 'etlReceipts' not in artifact.root['spec'] and not (target / 'etl_receipts.parquet').exists()
    assert any(i['role'] == 'prior-generation' and i['artifactDigest'] == args['expected_prior']
               for i in artifact.root['inputs'])
    for name in POLICIES:
        expected = pq.read_table(args['prior_directory'] / (name + '.parquet')).to_pylist()
        expected += pq.read_table(args['candidate_directory'] / name / (name + '.parquet')).to_pylist()
        assert pq.read_table(target / (name + '.parquet')).to_pylist() == expected
    assert all((target / name).stat().st_ino == inode for name, inode in captured.items())
    assert all((p.stat().st_ino, digest(p)) == pin for p, pin in before.items())
    assert result['payloadCopies'] == 0 and result['published'] is False and result['sourceReads'] == 1
    # The accepted original is the evidence's one publisher original, under its OLRC URL, never a local path.
    journal = [json.loads(line) for line in (Path(result['evidence']) / JOURNAL).read_text().splitlines()]
    assert [(e['sha256'], e['original_location']) for e in journal if is_original(e)] == [(
        digest(args['candidate_directory'] / 'original.zip'),
        'https://uscode.house.gov/download/releasepoints/us/pl/119/138/xml_uscAll@119-138.zip')]
    assert str(tmp_path) not in json.dumps(journal)


def test_a_prior_without_receipts_takes_the_next_addition(tmp_path):
    first = Path(addition.prepare_addition(**setup(tmp_path))['generation'])
    snapshot, expected_prior = entry(first, "2026-11-11T00:00:00Z")
    candidate = tmp_path / 'later'
    _, audit = native(candidate, 'later')
    result = addition.prepare_addition(prior_directory=first, candidate_directory=candidate, audit_pin=audit,
                                       read_snapshot=snapshot, expected_prior=expected_prior,
                                       output=tmp_path / 'second')
    editions = pq.read_table(Path(result['generation']) / 'uscode_editions.parquet').column('edition_id')
    assert editions.to_pylist() == ['old-0', 'new-0', 'later-0']


def test_refuses_overlap_without_replacing_prior(tmp_path):
    args = setup(tmp_path, new_prefix='old')
    prior_pin = digest(args['prior_directory'] / 'uscode_editions.parquet')
    with pytest.raises(ValueError, match='Overlapping or duplicate'):
        addition.prepare_addition(**args)
    assert digest(args['prior_directory'] / 'uscode_editions.parquet') == prior_pin
    assert not (args['output'] / 'generation/artifact.json').exists()


@pytest.mark.parametrize('change', ['prior', 'membership', 'declaration', 'audit', 'member', 'schema', 'original'])
def test_refuses_wrong_prior_membership_audit_content_or_schema(tmp_path, change):
    args = setup(tmp_path)
    if change == 'prior':
        args['expected_prior'] = 'sha256:' + 'f' * 64
    elif change == 'membership':
        args['read_snapshot']['families'][addition.FAMILY]['tables'].pop('uscode_editions.parquet')
    elif change == 'declaration':
        args['read_snapshot']['families'][addition.FAMILY]['tables']['uscode_editions.parquet']['rows'] = 999
    elif change == 'audit':
        args['audit_pin']['sha256'] = 'sha256:' + 'f' * 64
    elif change == 'original':
        (args['candidate_directory'] / 'original.zip').write_bytes(b'other publisher bytes')
    else:
        path = args['candidate_directory'] / 'uscode_editions/uscode_editions.parquet'
        if change == 'member':
            path.write_bytes(b'changed')
        else:
            pq.write_table(pa.table({'edition_id': ['new-0']}), path)
            build = args['candidate_directory'] / 'rebuild.json'
            value = json.loads(build.read_text())
            value['outputs']['uscode_editions']['sha256'] = digest(path)
            build.write_text(json.dumps(value))
            audit = Path(args['audit_pin']['path'])
            value = json.loads(audit.read_text())
            value['evidence_sha256'].update({str(path): digest(path), str(build): digest(build)})
            audit.write_text(json.dumps(value))
            args['audit_pin'].update(sha256=digest(audit), bytes=audit.stat().st_size)
    with pytest.raises((ValueError, KeyError)):
        addition.prepare_addition(**args)
    # The original is read once, while it is retained, so a changed one refuses inside the fresh output.
    assert not (args['output'] / 'generation').exists() if change == 'original' else not args['output'].exists()


def test_ordered_union_crosses_batch_boundaries_and_checks_multiplicity(tmp_path):
    schema = pa.schema([('value', pa.string()), ('nested', pa.list_(pa.string()))])
    first, second, target = [tmp_path / n for n in ['first.parquet', 'second.parquet', 'target.parquet']]
    rows = [{'value': None if i % 3 == 0 else '', 'nested': [None, str(i)]} for i in range(65)]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), first)
    pq.write_table(pa.Table.from_pylist(rows[:3], schema=schema), second)
    addition._union([first, second], schema, target)
    addition._check_ordered_union([first, second], target, schema)
    pq.write_table(pa.Table.from_pylist(rows + rows[:2], schema=schema), target)
    with pytest.raises(ValueError, match='omitted or added'):
        addition._check_ordered_union([first, second], target, schema)
    pq.write_table(pa.Table.from_pylist(list(reversed(rows)) + rows[:3], schema=schema), target)
    with pytest.raises(ValueError, match='value or occurrence order'):
        addition._check_ordered_union([first, second], target, schema)


def test_refuses_existing_output_and_input_drift(tmp_path, monkeypatch):
    args = setup(tmp_path)
    args['output'].mkdir()
    with pytest.raises(ValueError, match='fresh separate'):
        addition.prepare_addition(**args)
    args['output'].rmdir()
    original = generations.build_generation

    def build(directory, **kwargs):
        result = original(directory, **kwargs)
        path = args['prior_directory'] / 'uscode_editions.parquet'
        path.touch()
        return result

    monkeypatch.setattr(generations, 'build_generation', build)
    with pytest.raises(ValueError, match='Retained input changed'):
        addition.prepare_addition(**args)
    assert not (args['output'] / 'prepared.json').exists()


def test_union_coalesces_read_batches_into_bounded_row_groups(tmp_path):
    schema = pa.schema([('ordinal', pa.int64()), ('value', pa.string())])
    first, second, target = [tmp_path / n for n in ['first.parquet', 'second.parquet', 'target.parquet']]
    total = addition.ROW_GROUP_ROWS + 17
    rows = [{'ordinal': i, 'value': None if i % 2 else ''} for i in range(total)]
    pq.write_table(pa.Table.from_pylist(rows[:65], schema=schema), first, row_group_size=32)
    pq.write_table(pa.Table.from_pylist(rows[65:], schema=schema), second, row_group_size=32)
    addition._union([first, second], schema, target)
    addition._check_ordered_union([first, second], target, schema)
    with pq.ParquetFile(target) as parquet:
        assert parquet.metadata.num_row_groups == 2
        assert [parquet.metadata.row_group(i).num_rows for i in range(2)] == [addition.ROW_GROUP_ROWS, 17]
    assert pq.read_table(target).to_pylist() == rows
