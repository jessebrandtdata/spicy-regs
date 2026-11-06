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
    from spicy_regs.transforms.regulations_receipts import policy as regulations_policy, SOURCE_COLUMNS
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
    """Each installed declaration as written: the policy's descriptor, and the marker on a shared log."""
    from spicy_regs.subject_catalog import SHARED_LOG, is_shared_log

    shared = {policy.dataset for policy in shared_logs()}
    result = {name: {**policy.descriptor(), **({SHARED_LOG: True} if name in shared else {})}
              for name, policy in family_policies(datasets).items()}
    if {name for name, declared in result.items() if is_shared_log(declared)} != shared & result.keys():
        raise ValueError('A shared log has no declared policy')
    return result


def main():
    directory = ROOT / 'src/spicy_regs/etl_policies'
    policies = declarations()
    for name, declared in policies.items():
        (directory / f'{name}.json').write_text(json.dumps(declared, indent=2) + '\n')
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
    print(f'Updated {len(policies)} explicit policies')

if __name__ == '__main__':
    main()
