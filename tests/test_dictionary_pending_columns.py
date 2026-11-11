"""The live dictionary check between a column promotion's merge and the nightly that publishes it (ss-vse3).

``deploy-docs.yml`` runs ``spicy-regs-dict check --source r2`` on every push. A promoted column is described and
declared from the merge on, and the live file gains it only when its family next publishes: 12 failures for the two
Register tables' six columns, as for bills on 2026-11-09. These hold the allowance to exactly that case.
"""

from dataclasses import replace

import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import receipt_policies

PROMOTED = {"federal_register": ["body_html_url", "html_url", "pdf_url", "regulations_dot_gov_info_json", "source_sha256"],
            "fr_docket_links": ["normalized_docket_candidates"]}
FAMILY = {"federal_register": "federal-register", "fr_docket_links": "fr-docket-links"}


def published(table, *, written=1, receipts=True):
    """An index and root for ``table`` as its family last published it: under its ``written``-th earlier policy."""
    policy = receipt_policies(installed_policies()[table])[written]
    entry = {"prefix": f"generations/{FAMILY[table]}/x", "tables": {table + ".parquet": {}},
             **({"etlReceipts": {"datasets": [table]}} if receipts else {})}
    index = {"families": {FAMILY[table]: entry}}
    held = set(policy.subject_schema.names)
    live = {table: [pair for pair in dd.expected_schemas()[table] if pair[0] in held]}
    reads = []

    def load_root(base_url, read):
        reads.append(read["prefix"])
        return b"", {"spec": {"etlReceipts": {"policies": [policy.descriptor()]}}}

    return index, live, load_root, reads, policy


def errors(index, live, load_root):
    descriptions = {name: value for name, value in dd.load_descriptions(dd.DEFAULT_DESCRIPTIONS).items() if name in live}
    expected = {name: value for name, value in dd.expected_schemas().items() if name in live}
    before = dd.check_descriptions(live, descriptions) + dd.check_schema_drift(expected, live)
    descriptions, expected, notices = dd.accept_pending_policy_columns(
        index, "https://example.invalid", descriptions, expected, live, load_root=load_root)
    return before, dd.check_descriptions(live, descriptions) + dd.check_schema_drift(expected, live), notices


@pytest.mark.parametrize("table", sorted(PROMOTED))
def test_a_column_the_installed_policy_adds_to_the_published_one_is_a_notice(table):
    index, live, load_root, reads, policy = published(table)
    before, after, notices = errors(index, live, load_root)
    # Two failures a column: described but not in the file, and declared in code but not in the file.
    assert sorted(before) == sorted(
        [f"[{table}] column {name!r} in descriptions.yaml but missing from schema" for name in PROMOTED[table]]
        + [f"[{table}] column {name!r} in in-code schema but missing from live parquet" for name in PROMOTED[table]])
    assert after == []
    assert notices == [f"[{table}] declared, not yet published: {', '.join(PROMOTED[table])} (the selected generation "
                       f"states {policy.policy_version}; installed is {installed_policies()[table].policy_version})"]
    assert reads == [f"generations/{FAMILY[table]}/x"]


def test_a_column_the_published_policy_holds_is_still_missing():
    """The same generation without ``title``: the pending five are set aside, the lost column is not."""
    index, live, load_root, _, _ = published("federal_register")
    live["federal_register"] = [pair for pair in live["federal_register"] if pair[0] != "title"]
    _, after, notices = errors(index, live, load_root)
    assert sorted(after) == ["[federal_register] column 'title' in descriptions.yaml but missing from schema",
                             "[federal_register] column 'title' in in-code schema but missing from live parquet"]
    assert len(notices) == 1 and "title" not in notices[0]


def test_a_changed_type_or_an_undeclared_column_still_fails():
    index, live, load_root, _, _ = published("federal_register")
    live["federal_register"] = [(name, "VARCHAR" if name == "significant" else kind)
                                for name, kind in live["federal_register"]] + [("undeclared", "VARCHAR")]
    _, after, _ = errors(index, live, load_root)
    assert "[federal_register.significant] in-code type BOOLEAN but live parquet VARCHAR" in after
    assert "[federal_register] column 'undeclared' in live parquet but missing from in-code schema" in after


def test_only_a_declared_earlier_policy_of_a_family_with_receipts_is_recognised():
    index, live, load_root, _, policy = published("federal_register")
    unknown = replace(policy, policy_version="fixture/9")
    before, after, notices = errors(index, live, lambda base, entry: (
        b"", {"spec": {"etlReceipts": {"policies": [unknown.descriptor()]}}}))
    assert after == before and notices == []
    # A family that states no receipts declares no policy to compare, and its root is not read.
    index, live, load_root, reads, _ = published("federal_register", receipts=False)
    before, after, notices = errors(index, live, load_root)
    assert after == before and notices == [] and reads == []
    # A generation under the installed policy has nothing pending: a missing column there is a missing column.
    index, live, load_root, _, _ = published("federal_register", written=0)
    live["federal_register"] = live["federal_register"][:-1]
    before, after, notices = errors(index, live, load_root)
    assert after == before and len(before) == 2 and notices == []


#: The court tables whose fields left their receipts (ss-vse3), with the live generations' own roots (committed for
#: the archive pins): the columns stay pending until each family publishes without receipts.
COURT_ROOTS = {"court-citations": "7c2fc74c1b8096801f0f779baceb77e538e34047f234afbd9e56aa6515261d9d",
               "court-opinions": "85589a33b63f22bfac522e5f2c41210eb69437b763b81380562eda1fd33ef298"}
COURT_PROMOTED = {"court_citations": ["date_created", "date_modified", "dump_date", "source_sha256"],
                  "court_citation_map": ["dump_date", "source_sha256"],
                  "court_parentheticals": ["dump_date", "source_sha256"],
                  "court_opinions": ["date_created", "date_modified", "dump_date", "extracted_by_ocr", "local_path",
                                     "source_sha256"]}


def test_the_court_promotions_are_notices_against_the_live_generations_own_roots():
    import gzip
    import json
    from pathlib import Path

    roots = {family: json.loads(gzip.decompress((Path(__file__).parent / "fixtures/receipt_archive" /
                                                 f"{digest}.json.gz").read_bytes()))
             for family, digest in COURT_ROOTS.items()}
    index = {"families": {family: {"prefix": f"generations/{family}/{COURT_ROOTS[family]}",
                                   "tables": root["spec"]["tables"], "etlReceipts": {"datasets": [
                                       policy["dataset"] for policy in root["spec"]["etlReceipts"]["policies"]]}}
                          for family, root in roots.items()}}
    # Each live table holds exactly the columns its root's policy declared.
    live = {}
    for root in roots.values():
        for policy in root["spec"]["etlReceipts"]["policies"]:
            from spicy_regs.etl_receipts import DatasetPolicy
            from spicy_regs.native_types import described_schema
            declared = DatasetPolicy.from_descriptor(policy)
            live[declared.dataset] = described_schema(declared.subject_schema)
    assert set(live) == set(COURT_PROMOTED)

    def load_root(base_url, read):
        return b"", roots[read["prefix"].split("/")[1]]

    _, after, notices = errors(index, live, load_root)
    assert after == []
    assert sorted(notices) == sorted(
        f"[{table}] declared, not yet published: {', '.join(columns)} (the selected generation states "
        f"{'courts/2' if table == 'court_opinions' else 'courts/1'}; installed is "
        f"{installed_policies()[table].policy_version})" for table, columns in COURT_PROMOTED.items())
