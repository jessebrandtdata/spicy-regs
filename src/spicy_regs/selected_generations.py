"""One immutable native subject/receipt selection for local and published builds."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
from uuid import uuid4

from spicy_regs.sources import publication
from spicy_regs.runtime_bounds import stream_sha256


def _pin(path):
    with Path(path).open("rb") as stream:
        return "sha256:" + stream_sha256(stream)


def _member_pin(path, checked):
    """Hash unique regular bytes once within this call/session; refuse concurrent mutation."""
    from spicy_regs.local_data import file_signature

    path = Path(path).absolute()
    before = file_signature(path)
    held = checked.get(path)
    if held is not None:
        if held[0] != before:
            raise ValueError("Selected native member changed during its read session")
        return held[1]
    digest = _pin(path)
    if file_signature(path) != before:
        raise ValueError("Selected native member changed while hashing")
    checked[path] = (before, digest)
    return digest


#: A ranged receipt read falls back to a whole download above either bound on its selected groups' stored bytes:
#: past a quarter of the member, ranges save little; past 256 MiB, one whole streamed download is the simpler read.
RANGED_MAX_SHARE = 0.25
RANGED_MAX_BYTES = 256 * 1024 * 1024
#: Stored bytes pre-buffered at once by a ranged read (one row group may exceed it). Measured on the 214 MB
#: bill_publisher_summaries selection (2026-11-08), peak RSS: unbounded 870 MB, 64 MiB 826, 16 MiB 716 at the same
#: 15 s, 8 MiB 631 at 21 s; the process baseline with the 30 MB bill-family footer parsed is 443 MB.
RANGED_CHUNK_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class SelectedDataset:
    """One dataset's pinned subjects and the receipts beside them.

    ``receipts`` and ``generation_id`` are ``None`` for a table whose receipts are retired
    (``subject_catalog.RECEIPTS_RETIRED``): its pinned public table is the whole selection.
    """

    dataset: str
    subjects: tuple[Path, ...]
    receipts: Path | None
    generation_id: str | None

    def required_receipts(self, reader: str) -> Path:
        """The selected receipts, for a reader that still reads them; a retired table's refuses by name."""
        if self.receipts is None:
            raise ValueError(f"{self.dataset} has retired receipts; {reader} still reads them")
        return self.receipts


def unique_build_directory(root: Path) -> Path:
    directory = Path(root) / ".builds" / uuid4().hex
    directory.mkdir(parents=True)
    return directory


def remember_selection(root: Path, selections) -> None:
    """Pin immutable admitted members; mutable public convenience copies are never priors."""
    _remember_selection(root, selections, {})


def _remember_selection(root, selections, checked):
    directory = Path(root) / ".native-state"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "selection.json"
    values = json.loads(target.read_text()) if target.exists() else {}
    for selected in selections:
        if selected.receipts is None:
            # A table without receipts is never a native selection: it is read where its plain rollup wrote it.
            values.pop(selected.dataset, None)
            continue

        def member(path):
            return {"path": str(Path(path).resolve()), "sha256": _member_pin(path, checked)}

        values[selected.dataset] = {
            "generation_id": selected.generation_id,
            "subjects": [member(p) for p in selected.subjects],
            "receipts": member(selected.receipts),
        }
    temporary = target.with_name(target.name + "." + uuid4().hex)
    temporary.write_text(json.dumps(values))
    temporary.replace(target)


class SelectedInputs:
    """A captured remote index is authoritative; otherwise select pinned local native data.

    Pass public_url="" for an explicitly local run, even with environment credentials.
    Missing native receipts refuse: operational readers never migrate old tables.
    """

    def __init__(self, root, directory, *, index=None, public_url=None):
        self.root, self.directory = Path(root), Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.public_url = os.getenv("R2_PUBLIC_URL") if public_url is None else public_url
        self.index = (
            index if index is not None else (publication.current_index(self.public_url) if self.public_url else None)
        )
        pointer = self.root / ".native-state" / "selection.json"
        self.local = json.loads(pointer.read_text()) if self.index is None and pointer.exists() else {}
        self.cache = {}
        self.ranged = {}
        self.footers = {}
        self.checked = {}
        self.fetched = {}

    def member_pin(self, path):
        """Return a byte pin from this captured session, checking that its bytes remain unchanged."""
        digest = _member_pin(path, self.checked)
        signature, _ = self.checked[Path(path).absolute()]
        return {"sha256": digest, "byteSize": signature[2]}

    def adopt_download(self, path, downloaded):
        """Hold a member this session fetched against its pin, without hashing its bytes a second time."""
        from spicy_regs.local_data import file_signature

        path = Path(path).absolute()
        signature = file_signature(path)
        if signature != list(downloaded.state[:5]):
            raise ValueError("Selected native member changed after its download")
        self.checked[path] = (signature, downloaded.sha256)

    def receipts_pin(self, dataset, path):
        """The pin of the receipt member ``dataset`` was selected from: the published member's for a ranged read.

        A ranged selection is a local derivative no published object holds, so lineage keeps naming the member it
        was read from, exactly as a whole read does; the local selection is still held unchanged. For a ranged read
        that pin is the index's, not a digest of bytes read: the whole-file SHA-256 was not verified, and
        :meth:`receipts_verification` says so beside it.
        """
        if not isinstance(path, Path):
            raise ValueError(f"{dataset}: a split family's receipts have no single file to bind")
        local = self.member_pin(path)
        return dict(self.ranged[dataset]) if dataset in self.ranged else local

    def receipts_verification(self, dataset):
        """``"ranged"`` when ``dataset``'s receipts were read by row group (whole-file SHA-256 not verified), else ``None``."""
        return "ranged" if dataset in self.ranged else None

    def check(self):
        """Refuse changes to any original or derived member used by this session."""
        for path in self.checked:
            _member_pin(path, self.checked)

    def select_scoped(self, datasets):
        """Select datasets together, splitting each shared receipt member only once."""
        from spicy_regs.etl_receipts import select_receipt_family

        selected = {dataset: self.select(dataset) for dataset in datasets}
        groups = {}
        for dataset, value in selected.items():
            if value is not None:
                groups.setdefault(value.receipts, []).append(dataset)
        self.check()
        for receipts, names in groups.items():
            if receipts is None:
                continue
            scoped = select_receipt_family(receipts, self.directory / ("scoped-" + uuid4().hex), datasets=names)
            self.check()
            for dataset, path in scoped.items():
                self.member_pin(path)
                value = selected[dataset]
                assert value is not None
                selected[dataset] = replace(value, receipts=path)
        self.check()
        return selected

    def _ranged_receipts(self, member, dataset, target):
        """Only ``dataset``'s rows of one remote shared receipt member, read by row group, or ``None`` to fetch it whole.

        Reads the footer and the groups that can hold the dataset (a 19,767-row dataset of the 3.15 GB bill-family
        member: 34 MB). Fetches whole instead when this session already holds the member, or when those groups exceed
        :data:`RANGED_MAX_SHARE` of it or :data:`RANGED_MAX_BYTES`. A footer is parsed once per member and session.
        Accepted checks (owner decision 2026-11-08): the stored size and the footer's row count equal the pin, one
        ETag keeps the ranges consistent within the read, and the caller's admission checks each selected receipt's
        own digest against its pinned subjects. The whole-file SHA-256 is not verified.
        """
        import pyarrow.parquet as pq
        from spicy_regs.etl_receipts import RECEIPT_SCHEMA, dataset_row_groups, row_group_bytes, select_member_receipts

        if not self.public_url:
            raise ValueError(f"{dataset}: a ranged receipt read requires its public base URL")
        identity = (member.path, member.sha256, member.byte_size)
        if identity in self.fetched:
            return None
        with publication.open_member(self.public_url, member) as stream:
            # pre_buffer coalesces each run of selected groups' column chunks into a few range requests.
            with pq.ParquetFile(stream, metadata=self.footers.get(identity), pre_buffer=True) as parquet:
                if parquet.metadata.num_rows != member.rows:
                    raise ValueError(f"{dataset}: selected receipt member differs from its pinned row count")
                self.footers[identity] = parquet.metadata
                if not parquet.schema_arrow.equals(RECEIPT_SCHEMA):
                    return None
                groups = dataset_row_groups(parquet, dataset)
                stored = sum(row_group_bytes(parquet.metadata, group) for group in groups)
                if stored > min(RANGED_MAX_BYTES, RANGED_MAX_SHARE * member.byte_size):
                    return None
                select_member_receipts(parquet, target, dataset=dataset, row_groups=groups,
                                       chunk_bytes=RANGED_CHUNK_BYTES)
        _member_pin(target, self.checked)
        return target

    def select(self, dataset, *, ranged=False):
        """Pinned subjects and receipts of ``dataset``.

        ``ranged`` is for a caller that reads only ``dataset``'s receipts (every Congress reader selects them by
        dataset): a remote shared member may then be read for that dataset's rows alone (:meth:`_ranged_receipts`).
        """
        if dataset in self.cache:
            if dataset in self.ranged and not ranged:
                raise ValueError(f"{dataset}: receipts were selected for this dataset alone")
            selected = self.cache[dataset]
            for path in (*selected.subjects, *((selected.receipts,) if selected.receipts else ())):
                _member_pin(path, self.checked)
            return self.cache[dataset]
        from spicy_regs.subject_catalog import retired_receipt_datasets

        retired = dataset in retired_receipt_datasets()
        if self.index is None:
            value = self.local.get(dataset)
            published = self.root / f"{dataset}.parquet"
            if retired and published.is_file():
                # A local run beside the table a plain rollup just wrote: that file is the selection, pinned by hash.
                _member_pin(published, self.checked)
                self.cache[dataset] = SelectedDataset(dataset, (published,), None, None)
                return self.cache[dataset]
            if value is None:
                return None

            def checked(member):
                path = Path(member["path"])
                if _member_pin(path, self.checked) != member["sha256"]:
                    raise ValueError("Selected local native member changed")
                return path

            result = SelectedDataset(
                dataset,
                tuple(checked(p) for p in value["subjects"]),
                checked(value["receipts"]),
                value["generation_id"],
            )
        else:
            owner = publication.table_owner(self.index, dataset + ".parquet")
            owners = [
                f for f in self.index["families"].values() if dataset in f.get("etlReceipts", {}).get("datasets", ())
            ]
            if not owners and owner is None:
                return None
            # A retired table publishes no receipts: its pinned public table is read in their place.
            if len(owners) != 1 and not (retired and not owners):
                raise ValueError(f"{dataset}: selected native input requires one ETL receipt generation")
            if not self.public_url:
                raise ValueError("Selected remote inputs require their public base URL")
            public_url = self.public_url
            directory = self.directory / dataset
            directory.mkdir(parents=True, exist_ok=True)

            def fetch(member):
                from rulespec_artifacts import validate_object_key
                from spicy_regs.local_data import file_signature

                identity = (member.path, member.sha256, member.byte_size)
                held = self.fetched.get(identity)
                if held is not None:
                    target, signature = held
                    if file_signature(target) != signature:
                        raise ValueError("Selected remote native member changed during its read session")
                    return target
                # All datasets of one captured generation share its receipt member.
                # Its immutable object path preserves both family and generation.
                key = validate_object_key(member.path, path="selectedMember")
                target = self.directory / ".members" / key
                target.parent.mkdir(parents=True, exist_ok=True)
                downloaded = publication.fetch_member(public_url, member, target, member.path)
                if not downloaded:
                    raise ValueError(f"{dataset}: selected native member unavailable")
                if isinstance(downloaded, publication.DownloadedMember):
                    from spicy_regs.local_data import file_state_from_stat
                    if file_state_from_stat(target.lstat()) != downloaded.state:
                        raise ValueError("Selected native member changed after download verification")
                    digest = downloaded.sha256
                else:
                    # Injected fetch adapters must prove their final bytes too.
                    digest = _member_pin(target, self.checked)
                if digest != member.sha256:
                    raise ValueError("Selected remote native member differs from its pin")
                signature = file_signature(target)
                if isinstance(downloaded, publication.DownloadedMember):
                    if file_state_from_stat(target.lstat()) != downloaded.state:
                        raise ValueError("Selected native member changed during verification handoff")
                elif signature != self.checked[target.absolute()][0]:
                    raise ValueError("Selected native member changed during adapter verification handoff")
                if signature[2] != member.byte_size:
                    raise ValueError("Selected remote native member differs from its byte size pin")
                self.fetched[identity] = (target, signature)
                self.checked[target.absolute()] = (signature, digest)
                return target

            subjects = (
                tuple(fetch(m) for m in publication.table_members(self.index, dataset + ".parquet")) if owner else ()
            )
            if not owners:
                self.cache[dataset] = SelectedDataset(dataset, subjects, None, None)
                return self.cache[dataset]
            receipts = publication.receipt_members(self.index, dataset=dataset)
            if len(receipts) != 1:
                raise ValueError(f"{dataset}: ambiguous selected receipt member")
            specification = owners[0]["etlReceipts"]
            receipt_path = (self._ranged_receipts(receipts[0], dataset, directory / "receipts.parquet")
                            if ranged and len(receipts) == 1 else None)
            if receipt_path is not None:
                self.ranged[dataset] = {"sha256": receipts[0].sha256, "byteSize": receipts[0].byte_size}
            result = SelectedDataset(dataset, subjects, receipt_path or fetch(receipts[0]), specification["generationId"])
        self.cache[dataset] = result
        return result


def remember_generation(root: Path, directory: Path, artifact) -> None:
    """Select every dataset from one already admitted immutable generation."""
    from rulespec_artifacts import LocalFileState, LocalMemberSource, iter_member_descriptors
    from spicy_regs.local_data import file_signature
    from spicy_regs.sources.publication import member_table

    specification = artifact.root["spec"].get("etlReceipts")
    if specification is None:
        return
    members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
    selected = []
    checked = {}
    for member in members:
        if member.object_key is None:
            raise ValueError("Native generation member has no object key")
        path = directory / member.object_key
        state = (artifact.local_member_states or {}).get(member.object_key)
        if state is not None:
            signature = file_signature(path)
            if LocalFileState.from_stat(path.lstat()) != state:
                raise ValueError("Native generation changed after byte admission")
            checked[path.absolute()] = (signature, member.sha256)
    for policy in specification["policies"]:
        dataset = policy["dataset"]
        subjects = []
        for member in members:
            key = member.object_key
            if key is None:
                raise ValueError("Native generation member has no object key")
            if key != specification["key"] and member_table(key) == dataset + ".parquet":
                subjects.append(directory / key)
        selected.append(
            SelectedDataset(dataset, tuple(subjects), directory / specification["key"], specification["generationId"])
        )
    _remember_selection(root, selected, checked)
