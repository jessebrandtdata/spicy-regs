"""Build selected FEC metadata companions and reported relationships offline.

SpicyDocs verifies and parses the inputs. This adapter keeps complete provider
records, exact evidence coordinates, and the scope of every selected collection.
It never selects current amendments or combines overlapping source observations.
All outputs enter a new directory together after every input has been consumed.
"""

from __future__ import annotations

import importlib
import hashlib
import json
import re
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.fec_relationships import SCHEMA as RELATIONSHIP_SCHEMA
from spicy_regs.transforms.fec_relationships import api_relationships, bulk_relationships, statement_relationships

RECORD_COLUMNS = (
    "collection_id",
    "source_family",
    "profile",
    "source_record_id",
    "committee_id",
    "candidate_id",
    "filing_id",
    "legal_doc_id",
    "audit_case_id",
    "source_sha256",
    "source_url",
    "observed_at",
    "source_locator_json",
    "metadata_json",
    "assets_json",
    "embedded_bodies_json",
    "source_record_json",
)
COLLECTION_COLUMNS = (
    "collection_id",
    "source_family",
    "profile",
    "source_system_id",
    "source_state_scope",
    "record_count",
    "relationship_count",
    "record_outcome",
    "requested_scope_json",
    "coverage_limits_json",
    "collection_outcome_json",
    "artifact_sha256",
)
RECORD_SCHEMA = pa.schema([(name, pa.string()) for name in RECORD_COLUMNS])
COLLECTION_SCHEMA = pa.schema([(name, pa.int64() if name in {"record_count", "relationship_count"} else pa.string())
                              for name in COLLECTION_COLUMNS])
OUTPUTS = ("fec_source_records.parquet", "fec_collections.parquet", "fec_relationships.parquet")
_PROFILES = {
    "committee": ("profile", "FEC_COMMITTEE_CENSUS_PROFILE", "iter_retained_committee_pages"),
    "candidate": ("candidate_profile", "FEC_CANDIDATE_QUERY_PROFILE", "iter_retained_candidate_pages"),
    "filing": ("filing_profile", "FEC_FILING_QUERY_PROFILE", "iter_retained_filing_pages"),
    "legal": ("legal_profile", "FEC_LEGAL_QUERY_PROFILE", "iter_retained_legal_pages"),
    "audit": ("audit_profile", "FEC_AUDIT_QUERY_PROFILE", "iter_retained_audit_pages"),
    "bulk": ("bulk_profile", "FEC_BULK_FILES_PROFILE", None),
    "positional": ("row_profile", "FEC_POSITIONAL_ROWS_PROFILE", None),
    "agency": ("agency_profile", "FEC_AGENCY_REPORT_PROFILE", None),
    "agency-document": ("agency_document_profile", "FEC_AGENCY_DOCUMENT_PROFILE", None),
    "postgres": ("postgres_profile", "FEC_POSTGRES_ROWS_PROFILE", None),
    "document": ("document_profile", "FEC_DOCUMENT_PROFILE", None),
}
_ORIGINAL_ITERATORS = {
    "positional": "iter_retained_positional_rows",
    "agency": "iter_retained_agency_originals",
    "agency-document": "iter_retained_agency_documents",
    "postgres": "iter_retained_postgres_rows",
    "document": "iter_retained_documents",
}
_DISPOSITIONS = {"retained_unparsed", "refused", "unresolved", "inventory_only", "selection_context"}
_CONTEXT_FIELDS = {
    "version",
    "source_capture",
    "source_family",
    "bulk_group",
    "object_key",
    "member",
    "related_collection_ids",
    "source_authority",
    "selection",
    "parsing",
    "relationships",
    "evidence",
    "coverage_limits",
}
MAX_CONTEXT_BYTES = 1024 * 1024


def _context_pin(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"path", "sha256"}
        or not isinstance(value["path"], str)
        or not value["path"]
        or re.fullmatch(r"sha256:[0-9a-f]{64}", str(value["sha256"])) is None
    ):
        raise ValueError("FEC caller context requires a retained path and SHA-256")


def _caller_context(pin, base):
    """Read bounded, hash-verified source/coverage facts, separately from provider outcomes."""
    with _path(pin["path"], base).open("rb") as stream:
        raw = stream.read(MAX_CONTEXT_BYTES + 1)
    if len(raw) > MAX_CONTEXT_BYTES:
        raise ValueError("FEC caller context exceeds its byte limit")
    if "sha256:" + hashlib.sha256(raw).hexdigest() != pin["sha256"]:
        raise ValueError("FEC caller context SHA-256 differs")
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or set(value) - _CONTEXT_FIELDS
        or type(value.get("version")) is not int
        or value["version"] != 1
    ):
        raise ValueError("FEC caller context must contain version 1 source/coverage facts only")
    # Context is public metadata, never an arbitrary log or credential file.
    from spicy_docs.transport.credentials import scrub_credential

    def check(item):
        if isinstance(item, dict):
            if any(
                re.sub(r"[^a-z]", "", key.lower())
                in {
                    "apikey",
                    "token",
                    "accesstoken",
                    "authorization",
                    "password",
                    "secret",
                    "credential",
                }
                for key in item
            ):
                raise ValueError("FEC caller context cannot contain credential fields")
            for part in item.values():
                check(part)
        elif isinstance(item, list):
            for part in item:
                check(part)
        elif isinstance(item, str) and scrub_credential(item, "") != item:
            raise ValueError("FEC caller context cannot contain credential-bearing URLs")

    check(value)
    _json(value)  # Reject non-finite JSON numbers before emitting any row.
    return {"pin": pin, "facts": value}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _profile(name):
    module_name, constant, iterator = _PROFILES[name]
    module = importlib.import_module("spicy_docs.sources.fec." + module_name)
    return getattr(module, constant), getattr(module, iterator) if iterator else None


def _path(value, base):
    if not isinstance(value, str) or not value:
        raise ValueError("FEC input paths must be nonempty strings")
    path = Path(value)
    return path if path.is_absolute() else base / path


def _load_manifest(path):
    """Read a version-1 FEC manifest, refusing unknown families, profiles, modes or digests with ``ValueError``."""
    from spicy_docs.sources.fec.catalog import official_sources

    value = json.loads(path.read_bytes())
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "collections"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("expected FEC manifest version 1 and collections")
    collections = value["collections"]
    if not isinstance(collections, list) or not collections:
        raise ValueError("FEC collections must be a nonempty list")
    families = {item["id"] for item in official_sources()}
    seen = set()
    common = {"collection_id", "source_family", "profile", "blob_root"}
    for item in collections:
        if not isinstance(item, dict):
            raise ValueError("FEC collection must be an object")
        identity = item.get("collection_id")
        if not isinstance(identity, str) or not identity or identity in seen:
            raise ValueError("FEC collection_id must be nonempty and unique")
        seen.add(identity)
        if item.get("source_family") not in families:
            raise ValueError("unknown official FEC source family or reader profile")
        if "disposition" in item:
            disposition = item["disposition"]
            if (
                set(item) != {"collection_id", "source_family", "disposition"}
                or not isinstance(disposition, dict)
                or set(disposition) != {"status", "reason", "context"}
                or not isinstance(disposition["status"], str)
                or disposition["status"] not in _DISPOSITIONS
                or not isinstance(disposition["reason"], str)
                or not disposition["reason"].strip()
            ):
                raise ValueError(
                    "FEC metadata disposition requires explicit status, reason and context; no provider input"
                )
            _context_pin(disposition["context"])
            continue
        if "context" in item:
            _context_pin(item["context"])
        mode = (
            {"scope"}
            if "scope" in item
            else {"captures"}
            if "captures" in item
            else {"release_path", "artifact_sha256", "verifier_implementation_id"}
        )
        required = common | mode
        if set(item) - {"field_mapping", "context"} != required:
            raise ValueError("FEC collection fields differ from the selected input mode")
        if item["source_family"] not in families or item["profile"] not in _PROFILES:
            raise ValueError("unknown official FEC source family or reader profile")
        if "captures" in item and _PROFILES[item["profile"]][2] is None:
            raise ValueError("direct captures support API query profiles only")
        if "scope" in item and item["profile"] not in _ORIGINAL_ITERATORS:
            raise ValueError("direct scope requires a retained-original reader profile")
        if "field_mapping" in item:
            mapping = item["field_mapping"]
            if item["profile"] != "positional" or not isinstance(mapping, dict):
                raise ValueError("field_mapping requires positional input")
            dictionary = "dictionary" in mapping
            fields = (
                {"dictionary", "data_has_header"}
                if dictionary
                else {"header_collection_id", "header_row_ordinal", "data_has_header"}
            )
            if "relationship_family" in mapping:
                fields |= {"relationship_family", "cycle"}
                if (
                    mapping["relationship_family"]
                    not in {"linkage", "candidate_master", "committee_master", "form1_bulk", "leadership"}
                    or type(mapping.get("cycle")) is not int
                ):
                    raise ValueError("bulk relationships require an explicit supported family and cycle")
            if set(mapping) != fields or type(mapping["data_has_header"]) is not bool:
                raise ValueError("field_mapping requires an explicit dictionary or header collection and header flag")
            if dictionary:
                source = mapping["dictionary"]
                if (
                    not isinstance(source, dict)
                    or set(source) != {"capture", "blob_root"}
                    or mapping["data_has_header"]
                ):
                    raise ValueError("dictionary mapping requires a retained capture, blob root and no embedded header")
            elif (
                not isinstance(mapping["header_collection_id"], str)
                or not mapping["header_collection_id"]
                or type(mapping["header_row_ordinal"]) is not int
                or mapping["header_row_ordinal"] != 0
            ):
                raise ValueError("field_mapping requires a selected header collection and ordinal zero")
            elif mapping["data_has_header"] != (mapping["header_collection_id"] == item["collection_id"]):
                raise ValueError(
                    "embedded headers must name their own collection; external headers belong to an earlier collection"
                )
        if "artifact_sha256" in item and (
            not isinstance(item["artifact_sha256"], str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", item["artifact_sha256"]) is None
            or not isinstance(item["verifier_implementation_id"], str)
            or not item["verifier_implementation_id"]
        ):
            raise ValueError("release inputs require an expected digest and verifier implementation")
    return collections


def _release(item, base, profile):
    """Open a pinned SourceNative release, refusing one with failed or discarded records."""
    from rulespec_artifacts import ArtifactPin, LocalMemberSource
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    root = _path(item["release_path"], base)
    artifact = json.loads((root / "artifact.json").read_bytes())
    reader = SourceNativeReleaseReader(
        LocalMemberSource(root),
        blob_source=LocalSourceNativeBlobStore(_path(item["blob_root"], base)),
        profile=profile,
        expected_pin=ArtifactPin(logical_id=artifact["logicalId"], artifact_digest=item["artifact_sha256"]),
        accepted_verifier_implementation_ids=frozenset({item["verifier_implementation_id"]}),
    )
    if reader.collection_outcome["failedRecordCount"] or reader.collection_outcome["discardedObservationCount"]:
        raise ValueError("FEC companion delivery requires a release without failed or discarded records")
    return reader.iter_records(), reader.collection_outcome


def _query(item, base, profile, iterator):
    """Iterate a retained API-query capture set, refusing a page outside it and a repeated source identity."""
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    scope = profile.validate_query_scope({"captures": item["captures"]})
    outcome = {
        "requestedScope": scope,
        "sourceStateScope": profile.source_state_scope,
        "acquisitionPolicy": profile.acquisition_policy(scope),
        "verification": "retained-query-digests-membership-counts-and-source-identities",
    }

    def records():
        traversal = profile.traversal_check()
        seen = set()
        count = 0
        window = scope["captures"][0]["requestUrl"]
        for index, page in enumerate(
            iterator(scope["captures"], blob_source=LocalSourceNativeBlobStore(_path(item["blob_root"], base)))
        ):
            response = profile.parse_page_response(page.response_bytes)
            if not profile.records_included(response, query_scope=scope, page_window=window):
                raise ValueError("FEC query page falls outside the selected captures")
            traversal.add(response, page_index=index)
            for value in response["results"]:
                record = profile.classify_record(value)
                profile.validate_record_scope(record, query_scope=scope, page_window=window)
                wrapped = profile.wrap_record(record, schema_digest=profile.source_schema_digest())
                identity = wrapped["sourceRecordId"]
                if identity in seen:
                    raise ValueError("FEC selected query repeats a source identity")
                seen.add(identity)
                count += 1
                yield wrapped
        traversal.finish()
        outcome.update(
            recordOutcome="no-record-rejections" if count else "empty",
            publishedRecordCount=count,
            failedRecordCount=0,
            discardedObservationCount=0,
            acquisitionEvidenceCount=len(scope["captures"]),
        )

    return records(), outcome


def _original(item, base, profile):
    """Iterate one complete retained original, refusing an ordinal that skips or repeats."""
    from spicy_docs.sources.fec.delimited import held_original
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    module = importlib.import_module("spicy_docs.sources.fec." + _PROFILES[item["profile"]][0])
    iterator = getattr(module, _ORIGINAL_ITERATORS[item["profile"]])
    scope = profile.validate_query_scope(item["scope"])
    outcome = {
        "requestedScope": scope,
        "sourceStateScope": profile.source_state_scope,
        "acquisitionPolicy": profile.acquisition_policy(scope),
        "verification": "retained-original-digest-and-complete-selected-stream",
    }

    def records():
        store = LocalSourceNativeBlobStore(_path(item["blob_root"], base))
        (page,) = iterator(scope, blob_source=store)
        count = 0
        traversal = profile.traversal_check()
        # The whole original is verified before parsing, once per contiguous run of
        # its members: the session's held spool serves every member of one ZIP
        # (manifest order keeps them together). The provider verifies ZIP
        # membership/CRC and bounds; spooling avoids holding large files in RAM.
        original = held_original(store, page.blob_ref, page.byte_size)
        for index, response in enumerate(
            profile.parse_file_stream(
                original,
                query_scope=scope,
                request_key=page.request_key,
                evidence_ref=page.blob_ref,
                byte_size=page.byte_size,
                media_type=page.evidence_media_type,
            )
        ):
            traversal.add(response, page_index=index)
            for value in response["results"]:
                record = profile.classify_record(value)
                profile.validate_record_scope(record, query_scope=scope, page_window=0)
                if record["ordinal"] != count:
                    raise ValueError(f"FEC {item['profile']} stream skips or repeats an ordinal")
                count += 1
                yield profile.wrap_record(record, schema_digest=profile.source_schema_digest())
        traversal.finish()
        outcome.update(
            recordOutcome="no-record-rejections" if count else "empty",
            publishedRecordCount=count,
            failedRecordCount=0,
            discardedObservationCount=0,
            acquisitionEvidenceCount=1,
        )

    return records(), outcome


def _shape(item, wrapped, outcome):
    """Map one wrapped source record onto the published columns, returning the row and its evidence locator."""
    record = wrapped["record"]
    scope = outcome["requestedScope"]
    capture = record.get("capture", scope.get("capture"))
    if not capture:
        raise ValueError("FEC record has no source capture")
    locator = {"collection_id": item["collection_id"], "source_record_id": wrapped["sourceRecordId"]}
    metadata = record.get("metadata", record)
    if item["profile"] in {"positional", "postgres", "document"}:
        metadata = record["record"]
        if "source" in metadata:
            locator.update(metadata["source"])
        else:
            locator["embedded_bodies"] = metadata["embedded_bodies"]
        locator.update(member=record["member"], ordinal=record["ordinal"])
    elif item["profile"] in {"agency", "agency-document"}:
        metadata = record["record"]
        locator.update(ordinal=record["ordinal"], format=record["format"])
        if "member" in record:
            locator["member"] = record["member"]
    elif "source_pointer" in record:
        locator["json_pointer"] = record["source_pointer"]
    else:
        locator["object_key"] = capture["objectKey"]
    row = dict.fromkeys(RECORD_COLUMNS)
    agency = item["profile"] in {"agency", "agency-document"}
    named = metadata.get("metadata", metadata) if item["profile"] == "document" else metadata
    if not isinstance(named, dict):
        named = {}
    if item["profile"] == "postgres":
        # The source has checked the exact pg_restore schema and COPY stream.
        # Keep both native spellings and decoded values; array text stays text.
        names = [column["name"] for column in scope["derivation"]["columns"]]
        named = dict(zip(names, metadata["values"], strict=True))
        if len(named) != len(names):
            raise ValueError("FEC PostgreSQL schema repeats a column name")
        metadata = {**metadata, "named_fields": named}
        locator["column_schema_sha256"] = scope["derivation"]["outputs"]["schema"]["sha256"]
    row.update(
        collection_id=item["collection_id"],
        source_family=item["source_family"],
        profile=item["profile"],
        source_record_id=wrapped["sourceRecordId"],
        source_sha256=capture["responseSha256"],
        source_url=capture["requestUrl"],
        observed_at=capture["observedAt"],
        source_locator_json=_json(locator),
        metadata_json=_json(metadata),
        assets_json=_json(
            [metadata["asset"]]
            if agency and metadata.get("kind") == "oversight-asset"
            else metadata.get("assets", [])
            if item["profile"] == "document"
            else record.get("assets", [])
        ),
        embedded_bodies_json=_json(
            [metadata["body"]]
            if agency and metadata.get("kind") in {"oversight-body", "word-body"}
            else metadata.get("embedded_bodies", record.get("embedded_bodies", []))
        ),
        source_record_json=_json(wrapped),
    )
    for column, field in (
        ("committee_id", "committee_id"),
        ("candidate_id", "candidate_id"),
        ("filing_id", "sub_id"),
        ("legal_doc_id", "doc_id"),
        ("audit_case_id", "audit_case_id"),
    ):
        value = named.get(field)
        row[column] = value if isinstance(value, str) else None
    return row, locator


def _dictionary(item, base):
    """Read an explicitly selected source dictionary using the provider parser."""
    from spicy_docs.sources.fec.bulk_dictionary import MAX_DICTIONARY_BYTES, parse_bulk_dictionary
    from spicy_docs.sources.fec.originals import original_capture
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore, iter_verified_blob

    selected = item["field_mapping"]["dictionary"]
    capture = original_capture(selected["capture"])
    if capture["representation"] != "opaque" or capture["byteSize"] > MAX_DICTIONARY_BYTES:
        raise ValueError("dictionary requires a bounded opaque HTML original")
    store = LocalSourceNativeBlobStore(_path(selected["blob_root"], base))
    raw = b"".join(iter_verified_blob(store, capture["responseSha256"], capture["byteSize"]))
    definition = parse_bulk_dictionary(raw, sha256=capture["responseSha256"])
    evidence = {
        "kind": "official-html-dictionary",
        "source_sha256": capture["responseSha256"],
        "source_url": capture["requestUrl"],
        "observed_at": capture["observedAt"],
        "table_locator": definition["table_locator"],
        "definitions_collection_id": item["collection_id"],
        "definitions_pointer": "/tableFieldDefinitions",
    }
    return ([field["name"] for field in definition["fields"]], evidence), definition


def _named_fields(item, wrapped, row, locator, headers, dictionary=None):
    """Use only explicitly selected, byte-verified source header cells."""
    if item["profile"] != "positional":
        return None
    native = wrapped["record"]
    fields = native["record"].get("fields")
    if native["ordinal"] == 0 and isinstance(fields, list):
        headers[item["collection_id"]] = (
            fields,
            {
                "collection_id": item["collection_id"],
                "source_record_id": wrapped["sourceRecordId"],
                "source_sha256": row["source_sha256"],
                "source_url": row["source_url"],
                "source_locator": dict(locator),
            },
        )
    mapping = item.get("field_mapping")
    if mapping is None:
        return None
    selected = dictionary if dictionary is not None else headers.get(mapping["header_collection_id"])
    if selected is None:
        raise ValueError("selected field header must be present in its own or an earlier collection")
    names, evidence = selected
    if not names or any(not name for name in names) or len(set(names)) != len(names):
        raise ValueError("source field header names must be nonempty and unique")
    locator["field_mapping"] = evidence
    row["source_locator_json"] = _json(locator)
    if mapping["data_has_header"] and native["ordinal"] == 0 or fields == []:
        return None
    if not isinstance(fields, list) or len(fields) != len(names):
        raise ValueError("source row field count differs from its selected header")
    mapped = dict(zip(names, fields, strict=True))
    row["metadata_json"] = _json(mapped)
    # These are literal source ID fields, not validated entity classifications.
    # In particular, aggregate candidate placeholders remain source values.
    for column, aliases in (
        ("committee_id", ("committee_id", "CMTE_ID", "COMMITTEE_ID", "Committee_Id")),
        ("candidate_id", ("candidate_id", "CAND_ID", "CANDIDATE_ID", "Cand_Id", "cand_id")),
    ):
        values = [mapped[name] for name in aliases if name in mapped]
        if values:
            if len(set(values)) != 1:
                raise ValueError(f"conflicting source aliases for {column}")
            row[column] = values[0]
    return mapped


def _relationships(item, wrapped, row, locator, version, mapped):
    """Yield every relationship this record's profile and field mapping authorize."""
    evidence = {
        "sha256": row["source_sha256"],
        "url": row["source_url"],
        "observed_at": row["observed_at"],
        "locator": locator,
    }
    if mapped is not None and "relationship_family" in item["field_mapping"]:
        mapping = item["field_mapping"]
        yield from bulk_relationships(mapping["relationship_family"], mapped, evidence=evidence, cycle=mapping["cycle"])
    elif item["profile"] == "committee":
        yield from api_relationships(wrapped["record"]["metadata"], evidence=evidence)
    elif item["profile"] == "positional":
        record = wrapped["record"]["record"]
        fields = record.get("fields")
        if (
            isinstance(fields, dict)
            and fields.get("0") in {"F1N", "F1A", "F1S", "F2N", "F2A", "F2S"}
            and version in {"8.3", "8.4"}
        ):
            yield from statement_relationships(record, version=version, evidence=evidence)


def build_fec_observations(manifest: Path, output_dir: Path, *, batch_size: int = 2000) -> tuple[Path, ...]:
    """Verify selected inputs and install one new directory; existing outputs refuse.

    Relative input paths resolve beside the manifest. No HTTP request, prior-table
    merge, source mutation, or publication occurs. Replays use a new destination.
    """
    from spicy_docs.sources.fec.delimited import verified_archive_session

    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")
    manifest, output_dir = Path(manifest), Path(output_dir)
    if output_dir.exists():
        raise FileExistsError("FEC output directory already exists; select a new generation directory")
    collections = _load_manifest(manifest)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".fec-observations-", dir=output_dir.parent) as scratch:
        stage = Path(scratch) / "tables"
        stage.mkdir()
        schemas = (RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA)
        with ExitStack() as stack:
            stack.enter_context(verified_archive_session())
            writers = [
                stack.enter_context(pq.ParquetWriter(stage / name, schema, compression="zstd"))
                for name, schema in zip(OUTPUTS, schemas, strict=True)
            ]
            buffers = [[], [], []]
            headers = {}

            def append(index, row):
                buffers[index].append(row)
                if len(buffers[index]) >= batch_size:
                    flush(index)

            def flush(index):
                if buffers[index]:
                    writers[index].write_table(pa.Table.from_pylist(buffers[index], schema=schemas[index]))
                    buffers[index].clear()

            for number, item in enumerate(collections, 1):
                started = monotonic()
                logger.info("FEC collection {}/{}: {}", number, len(collections), item["collection_id"])
                if "disposition" in item:
                    disposition = item["disposition"]
                    context = _caller_context(disposition["context"], manifest.parent)
                    append(
                        1,
                        {
                            **dict.fromkeys(COLLECTION_COLUMNS),
                            "collection_id": item["collection_id"],
                            "source_family": item["source_family"],
                            "record_count": 0,
                            "relationship_count": 0,
                            "record_outcome": disposition["status"],
                            "collection_outcome_json": _json(
                                {
                                    "providerOutcome": None,
                                    "receiverDisposition": {
                                        "status": disposition["status"],
                                        "reason": disposition["reason"],
                                        "sourceRecordCount": None,
                                        "callerContext": context,
                                    },
                                }
                            ),
                        },
                    )
                    continue
                context = _caller_context(item["context"], manifest.parent) if "context" in item else None
                profile, iterator = _profile(item["profile"])
                dictionary, definitions = (
                    _dictionary(item, manifest.parent)
                    if "dictionary" in item.get("field_mapping", {})
                    else (None, None)
                )
                if "scope" in item:
                    records, outcome = _original(item, manifest.parent, profile)
                elif "captures" in item:
                    records, outcome = _query(item, manifest.parent, profile, iterator)
                else:
                    records, outcome = _release(item, manifest.parent, profile)
                record_count = relationship_count = 0
                version = None
                for wrapped in records:
                    row, locator = _shape(item, wrapped, outcome)
                    if item["profile"] == "positional":
                        native = wrapped["record"]["record"]
                        if native.get("kind") == "header":
                            version = native["format_version"]
                    mapped = _named_fields(item, wrapped, row, locator, headers, dictionary)
                    append(0, row)
                    record_count += 1
                    if record_count % 1_000_000 == 0:
                        logger.info(
                            "FEC collection {} progress: {} records in {:.1f}s",
                            item["collection_id"],
                            record_count,
                            monotonic() - started,
                        )
                    for relationship in _relationships(item, wrapped, row, locator, version, mapped):
                        append(2, relationship)
                        relationship_count += 1
                if record_count != outcome["publishedRecordCount"]:
                    raise ValueError("FEC output count differs from selected input membership")
                if "field_mapping" in item:
                    outcome = {**outcome, "tableFieldMapping": item["field_mapping"]}
                    if definitions is not None:
                        outcome["tableFieldDefinitions"] = definitions
                if context is not None:
                    outcome = {**outcome, "callerContext": context}
                append(
                    1,
                    {
                        "collection_id": item["collection_id"],
                        "source_family": item["source_family"],
                        "profile": item["profile"],
                        "source_system_id": profile.source_system_id,
                        "source_state_scope": outcome["sourceStateScope"],
                        "record_count": record_count,
                        "relationship_count": relationship_count,
                        "record_outcome": outcome["recordOutcome"],
                        "requested_scope_json": _json(outcome["requestedScope"]),
                        "coverage_limits_json": _json(outcome["acquisitionPolicy"].get("coverageLimits", [])),
                        "collection_outcome_json": _json(outcome),
                        "artifact_sha256": item.get("artifact_sha256"),
                    },
                )
                logger.info(
                    "FEC collection {} complete: {} records, {} relationships in {:.1f}s",
                    item["collection_id"],
                    record_count,
                    relationship_count,
                    monotonic() - started,
                )
            for index in range(3):
                flush(index)
        # A failed input never replaces any member of an earlier generation.
        stage.rename(output_dir)
    return tuple(output_dir / name for name in OUTPUTS)
