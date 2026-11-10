"""Generate installed public subject policies from explicit family declarations."""
from __future__ import annotations

import base64
import json
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]


def shared_logs():
    """The logs every family reading one source writes for itself; none owns one (``subject_catalog.SHARED_LOG``).

    Declare a log here only when each family's rows are its own run history, read back through that family's own
    subjects. A retry checkpoint or any dataset a reader finds by name belongs to one family and is not listed.
    """
    from spicy_regs.congress_receipts import ACQUISITION_POLICY
    from spicy_regs.legislative_receipts import FILE_POLICY
    return ACQUISITION_POLICY, FILE_POLICY


def family_policies(datasets: frozenset[str] | None = None):
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.court_receipts import POLICIES as courts
    from spicy_regs.legislative_receipts import policy as legislative_policy
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.scorecards.etl import POLICIES as scorecards
    from spicy_regs.transforms.government_receipts import POLICIES as government
    from spicy_regs.transforms.regulations_receipts import (
        PARENT_PINNED_SUMMARIES, SOURCE_COLUMNS, policy as regulations_policy,
    )
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy as identity_policy, REGISTRY
    from spicy_regs.transforms.fec_subject_receipts import dataset_policy as fec_policy
    from spicy_regs.transforms.fec_native_subjects import FIELD_RULES
    from spicy_regs.navigation_read_outcomes import POLICIES as navigation_outcomes

    from spicy_regs.sources.retained_native_collections import POLICIES as retained_native
    from spicy_regs.sources.retained_collections import POLICIES as administration
    from spicy_regs.transforms.historical_regulations import PUBLIC_POLICIES as historical
    from spicy_regs.sources.table3_history import POLICY as table3_history
    from spicy_regs.sources.uscode_editions import POLICIES as editions
    from spicy_regs.transforms.build_crs_report_files import POLICY as crs_files

    if datasets is not None and not datasets:
        raise ValueError("Selected dataset declarations must be nonempty")
    result = {}
    def add(policy):
        if datasets is not None and policy.dataset not in datasets:
            return
        old = result.get(policy.dataset)
        if old is not None and old.descriptor() != policy.descriptor():
            raise ValueError(f'Family policy conflict: {policy.dataset}')
        result[policy.dataset] = policy
    for policy in (*courts.values(), *scorecards.values(), *government.values(), *navigation_outcomes.values(), *shared_logs(),
                   *administration.values(), *retained_native.values(), *historical.values(), table3_history,
                   *editions.values(), crs_files):
        add(policy)
    for name in INPUT_COLUMNS:
        add(congress_policy(name))
    for name in field_registry():
        add(legislative_policy(name))
    for name in SOURCE_COLUMNS:
        if name not in PARENT_PINNED_SUMMARIES:
            add(regulations_policy(name))
    for name in REGISTRY:
        add(identity_policy(name))
    declarations = json.loads((ROOT / 'data_dictionary/fec_typed_schemas.json').read_text())['tables']
    for name in FIELD_RULES:
        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declarations[name]['arrow_schema_ipc_base64'])))
        add(fec_policy(name, schema))
    if datasets is not None and (missing := datasets - result.keys()):
        raise ValueError(f"Unknown dataset declarations: {sorted(missing)}")
    return result


def declarations(datasets: frozenset[str] | None = None):
    """Each installed declaration as written: the policy's descriptor, the marker on a shared log, and the marker on
    a table whose receipts are retired, kept from its installed file (its one owner; set it there by hand)."""
    from spicy_regs.subject_catalog import RECEIPTS_RETIRED, SHARED_LOG, is_shared_log, retired_receipt_datasets

    shared = {policy.dataset for policy in shared_logs()}
    retired = retired_receipt_datasets()
    result = {name: {**policy.descriptor(), **({SHARED_LOG: True} if name in shared else {}),
                     **({RECEIPTS_RETIRED: True} if name in retired else {})}
              for name, policy in family_policies(datasets).items()}
    if {name for name, declared in result.items() if is_shared_log(declared)} != shared & result.keys():
        raise ValueError('A shared log has no declared policy')
    return result


def record_migrations(declared, replays, *, ledger_path=None, installed=None):
    """Keep ``policy_migrations.json`` in step with the declarations about to be installed (ss-aj7t, ss-ag1j).

    A dataset whose descriptor changes must change its ``policy_version`` too; its previous installed descriptor
    becomes its newest earlier entry, which the new policy reads, unless ``replays`` gives a reason for it (then it is
    replay-required and needs a registered replay, ``policy_migrations.REPLAYS``). A new dataset is recorded as
    installed. A table whose receipts are retired is recorded like any other: the change that marks it may also
    promote columns, and its last night on receipts must still read its prior under the new policy.
    """
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.policy_migrations import LEDGER, REPLAY_REQUIRED, descriptor_digest
    from spicy_regs.subject_catalog import descriptor_of

    ledger_path = ledger_path or ROOT / 'src/spicy_regs' / LEDGER
    installed = installed or ROOT / 'src/spicy_regs/etl_policies'
    ledger = json.loads(ledger_path.read_text())
    unused = set(replays)
    for name, declaration in sorted(declared.items()):
        policy = DatasetPolicy.from_descriptor(descriptor_of(declaration))
        digest = descriptor_digest(policy)
        entry = ledger.setdefault(name, {'installed': digest})
        if entry['installed'] == digest:
            continue
        previous = descriptor_of(json.loads((installed / f'{name}.json').read_text()))
        if descriptor_digest(DatasetPolicy.from_descriptor(previous)) != entry['installed']:
            raise ValueError(f'{name}: its installed file is not the policy {LEDGER} records as current')
        if previous['policy_version'] == policy.policy_version:
            raise ValueError(f'{name}: its policy changed under {policy.policy_version}; give it a new policy_version '
                             'in its family\'s policy() (receipts name a policy by its version)')
        reason = replays.get(name)
        unused.discard(name)
        entry['earlier'] = [*entry.get('earlier', ()), {**previous, **({REPLAY_REQUIRED: reason} if reason else {})}]
        entry['installed'] = digest
    if unused:
        raise ValueError(f'--replay-required names datasets whose policy did not change: {sorted(unused)}')
    ledger_path.write_text(json.dumps(ledger, indent=1, sort_keys=True) + '\n')


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--replay-required', action='append', default=[], metavar='DATASET=REASON',
                        help='a changed dataset whose previous policy\'s rows are rebuilt from originals, not carried')
    args = parser.parse_args(argv)
    replays = dict(item.split('=', 1) for item in args.replay_required)
    directory = ROOT / 'src/spicy_regs/etl_policies'
    policies = declarations()
    record_migrations(policies, replays)
    rewritten = 0
    for name, declared in policies.items():
        path = directory / f'{name}.json'
        # Only a changed declaration is rewritten, so a run on an unchanged tree rewrites nothing (eight files
        # written with sorted keys on 2026-10-10, 02987594, differ from this layout in key order alone).
        if not path.exists() or json.loads(path.read_text()) != declared:
            path.write_text(json.dumps(declared, indent=2) + '\n')
            rewritten += 1
    stale = {p.stem for p in directory.glob('*.json')} - policies.keys()
    if stale:
        raise ValueError(f'Unowned installed policy declarations: {sorted(stale)}')
    from spicy_regs import data_dictionary as dd
    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.native_types import described_schema
    inputs = {name: arrow_schema(contract) for name, contract in dd._contracts().items() if name.startswith("fec_")}
    resource = json.loads((ROOT / "data_dictionary/fec_typed_schemas.json").read_text())["tables"]
    inputs.update({name: pa.ipc.read_schema(pa.BufferReader(base64.b64decode(item["arrow_schema_ipc_base64"])))
                   for name, item in resource.items()})
    import importlib
    for module in ("fec_candidate_observations", "fec_committee_observations"):
        inputs.update(importlib.import_module("spicy_regs.transforms." + module).SCHEMAS)
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_SCHEMA
    from spicy_regs.transforms.fec_relationships import SCHEMA as RELATIONSHIP_SCHEMA
    inputs.update(fec_collections=COLLECTION_SCHEMA, fec_source_records=RECORD_SCHEMA,
                  fec_relationships=RELATIONSHIP_SCHEMA)
    import yaml
    descriptions = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text())["tables"]
    payload = {}
    for name, schema in sorted(inputs.items()):
        entry = descriptions.get(name, {})
        prose = (dd.contract_column_prose(name) if entry.get("columns_from") == "spicy_docs" else
                 dict(entry.get("columns", {})))
        missing = set(schema.names) - prose.keys()
        if missing:
            raise ValueError(f"{name}: missing processing field descriptions: {sorted(missing)}")
        payload[name] = {"columns": described_schema(schema), "subject": entry["subject"],
                         "arrow_schema": base64.b64encode(schema.serialize().to_pybytes()).decode(),
                         "descriptions": {column: prose[column] for column in schema.names}}
    (ROOT / "src/spicy_regs/fec_processing_schemas.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(f'Rewrote {rewritten} of {len(policies)} explicit policies')

if __name__ == '__main__':
    main()
