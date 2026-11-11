"""Explicit conversion of complete published congressional shaped inputs."""

from collections.abc import Mapping, Sequence
from hashlib import file_digest, sha256
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.congress_receipts import CongressInput, write_congress_dataset
from spicy_regs.etl_receipts import combine_receipts, exact_json
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication


PROJECTED_NATIVE_OFFICIAL_TABLES = frozenset({"congress_bills", "amendments", "roll_call_votes"})


def published_input(entry: Mapping, name: str) -> bool:
    """Whether input ``name`` is read as its published table: its receipts are retired and its family lists none.

    Until that family's first generation without them, the receipts it still lists are read as before (ss-vse3).
    """
    from spicy_regs.subject_catalog import retired_receipt_datasets

    return name in retired_receipt_datasets() and name not in entry.get("etlReceipts", {}).get("datasets", ())


def read_published_native_resolver_rows(snapshot, name, paths):
    """Read complete pinned main fields when the resolver needs no processing fields.

    Member aliases and term observations still use the receipt-backed reader:
    native structs cannot distinguish an omitted source property from null.
    This path admits exact native schemas, bytes and populations, and does not
    claim to revalidate source receipts or reconstruct the original source row.
    """
    from spicy_regs.congress_receipts import input_policy
    from spicy_regs.local_data import file_signature, file_state_from_stat
    if name not in PROJECTED_NATIVE_OFFICIAL_TABLES:
        raise ValueError("Resolver main-field reads are not qualified for: " + name)
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or (name not in owner[1].get("etlReceipts", {}).get("datasets", ())
                         and not published_input(owner[1], name)):
        raise ValueError("Resolver main-field reads require a selected native publication")
    # The installed schema or an earlier one the ledger records: a table that promoted columns still reads its
    # generations from before (policy_migrations).
    schema = input_policy(name, tuple(path.resolve(strict=True) for path in paths)).subject_schema
    members = publication.table_members(snapshot, name + ".parquet")
    if not paths or len(paths) != len(members):
        raise ValueError("Resolver main-field reads require every selected member")
    rows = []
    for path, member in zip(paths, members, strict=True):
        # Explicit local-input staging links to retained bytes. Bind both the
        # staged locator and its regular target while reading the target.
        staged_before = file_state_from_stat(path.lstat())
        retained = path.resolve(strict=True)
        before = file_signature(retained)
        with retained.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if digest != member.sha256 or retained.stat().st_size != member.byte_size:
            raise ValueError("Resolver main input differs from its selected immutable pin")
        with pq.ParquetFile(retained) as source:
            if not source.schema_arrow.equals(schema, check_metadata=False):
                raise ValueError("Resolver main input differs from its exact native schema")
            for batch in source.iter_batches(batch_size=2000, columns=list(OFFICIAL_COLUMNS[name])):
                rows.extend(batch.to_pylist())
        if (file_state_from_stat(path.lstat()) != staged_before
                or path.resolve(strict=True) != retained or file_signature(retained) != before):
            raise ValueError("Resolver main input changed during its read")
    if len(rows) != owner[1]["tables"][name + ".parquet"]["rows"]:
        raise ValueError("Resolver main input differs from the complete selected population")
    return rows


def _rows(paths):
    for path in paths:
        with pq.ParquetFile(path) as reader:
            for batch in reader.iter_batches(batch_size=2000):
                yield from batch.to_pylist()


def _authored_footer(original: Mapping[bytes, bytes], restored: Mapping[bytes, bytes]) -> list[str]:
    """Preserve each original entry; report only the generated Arrow addition.

    Decoded schema equality is checked separately. This does not assert an
    identical physical Parquet encoding or permit rewriting an original entry.
    """
    if any(restored.get(key) != value for key, value in original.items()):
        raise ValueError("Published official conversion changed an original footer entry")
    added = set(restored) - set(original)
    if added - {b"ARROW:schema"}:
        raise ValueError("Published official conversion added unknown footer entries")
    return sorted(key.decode() for key in added)


def convert_published_official_input(
    snapshot: Mapping,
    name: str,
    paths: Sequence[Path],
    directory: Path,
    *,
    generation_id: str,
    published_url: str,
) -> tuple[CongressInput, dict]:
    """Convert pinned shaped bytes, admitting every native subject and receipt.

    The original public pins remain analysis parents. These witnesses name
    published shaped data, never an original publisher HTTP response. Refused
    conversions, incomplete populations, schema/authored-footer changes and changed
    values stop the preparation. Native published inputs use their own existing
    receipt path instead of this explicitly selected conversion.
    """
    if name not in OFFICIAL_COLUMNS or not generation_id or not published_url:
        raise ValueError("Published official conversion requires an explicit dataset, generation and source URL")
    owner = publication.table_owner(snapshot, name + ".parquet")
    if owner is None or "etlReceipts" in owner[1]:
        raise ValueError("Convert only explicitly selected published shaped official inputs")
    members = publication.table_members(snapshot, name + ".parquet")
    if not paths or len(paths) != len(members):
        raise ValueError("Published conversion requires every selected table member")
    schema = pq.read_schema(paths[0])
    original_footer = pq.read_metadata(paths[0]).metadata or {}
    if set(OFFICIAL_COLUMNS[name]) - set(schema.names):
        raise ValueError("Published official input lacks required resolver fields: " + name)
    for path, member in zip(paths, members, strict=True):
        with path.open("rb") as stream:
            digest = "sha256:" + file_digest(stream, "sha256").hexdigest()
        if digest != member.sha256 or path.stat().st_size != member.byte_size:
            raise ValueError("Published conversion input differs from its immutable pin")
        if not pq.read_schema(path).equals(schema, check_metadata=True):
            raise ValueError("Published input members have inconsistent source schemas or footers")
        if (pq.read_metadata(path).metadata or {}) != original_footer:
            raise ValueError("Published input members have inconsistent original footer entries")
    directory.mkdir(parents=True, exist_ok=False)
    subjects, receipts, witnesses = [], [], []
    for ordinal, (path, member) in enumerate(zip(paths, members, strict=True)):
        witness = dict(source_id="published-shaped-observation:" + name,
                       source_uri=published_url.rstrip("/") + "/" + member.path,
                       sha256=member.sha256, locator=member.key, body_version=None)
        subject, receipt = write_congress_dataset(
            path, directory / f"member-{ordinal}", dataset=name,
            generation_id=generation_id, witnesses=[witness],
        )
        if subject is None:
            raise ValueError("Official conversion produced no domain subject table")
        subjects.append(subject)
        receipts.append(receipt)
        witnesses.append(witness)
    combined = combine_receipts(receipts, directory / "etl_receipts.parquet")
    if any(row["outcome"] == "refused" for row in _rows([combined])):
        raise ValueError("Published official conversion has refused native values; retained attempts require review")
    selected = CongressInput(tuple(subjects), combined, generation_id)
    restored = selected.materialize(name, directory / "restored.parquet")
    if not pq.read_schema(restored).equals(schema, check_metadata=True):
        raise ValueError("Published official conversion changed source schema metadata")
    generated_footer_keys = _authored_footer(original_footer, pq.read_metadata(restored).metadata or {})
    before, after, count = sha256(), sha256(), 0
    absent = object()
    for original, returned in zip_longest(_rows(paths), _rows([restored]), fillvalue=absent):
        if original is absent or returned is absent:
            raise ValueError("Published official conversion changed the complete source population")
        left, right = exact_json(original), exact_json(returned)
        if left != right:
            raise ValueError("Published official conversion changed an original source field")
        before.update((left + "\n").encode())
        after.update((right + "\n").encode())
        count += 1
    declared = owner[1]["tables"][name + ".parquet"]["rows"]
    if count != declared:
        raise ValueError("Published official conversion population differs from the selected table count")
    return selected, dict(dataset=name, generation_id=generation_id, rows=count,
                         original_table_pin=publication.table_pin(snapshot, name + ".parquet"),
                         original_rows_sha256=before.hexdigest(), restored_rows_sha256=after.hexdigest(),
                         exact_schema_and_authored_footer=True, generated_footer_keys_added=generated_footer_keys,
                         required_resolver_columns=list(OFFICIAL_COLUMNS[name]),
                         witnesses=witnesses, source_acquisition_requests=0,
                         scope="Verified conversion of complete published shaped inputs; no original acquisition is asserted")
