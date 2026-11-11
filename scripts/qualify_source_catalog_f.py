"""Offline qualification over explicit retained GAO, Court and FCC captures.

This lane calls maintained parsing, traversal, shaping and native receipt
writers. It never fetches missing inputs or treats a baseline as a
publisher capture. A failed comparison remains readable in its new directory.
"""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
from types import ModuleType
from typing import Any, TypedDict

import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import ArtifactPin, admit_artifact


class RowComparison(TypedDict):
    expected_rows: int
    actual_rows: int
    missing_row_occurrences: int
    added_row_occurrences: int
    all_values_and_multiplicity_equal: bool
    ordered_rows_equal: bool


class CourtGroupResult(TypedDict):
    group_index: int
    capture_ids: list[str]
    source_occurrences: int
    accepted: int
    refused: int
    complete_empty_search: bool
    comparison: RowComparison
    all_outcomes_match: bool
    outputs: list[dict]


class CaptureOccurrence(TypedDict):
    journal_ordinal: int
    event: dict[str, Any]


def _module_path(module: ModuleType) -> Path:
    filename = getattr(module, "__file__", None)
    if not isinstance(filename, str) or not filename:
        raise ValueError("Qualification requires a module source-file path")
    return Path(filename)


def pin(path: Path) -> dict:
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"path": str(path.resolve()), "sha256": "sha256:" + digest,
            "byteSize": path.stat().st_size}


def assert_pin(path: Path, expected: dict) -> None:
    actual = pin(path)
    if any(actual[key] != expected[key] for key in ("sha256", "byteSize")):
        raise ValueError(f"Changed selected input: {path}")


class RetainedArtifact:
    """Adapt the canonical compressed object store to maintained admission."""

    def __init__(self, corpus: Path, digest: str, *, byte_budget: int = 256 * 1024 * 1024):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("Expected artifact SHA-256")
        self.corpus, self.digest = corpus, digest
        self.directory = corpus / "source-evidence" / digest[7:]
        self.root = json.loads((self.directory / "artifact.json").read_text())
        self.metadata = {"artifact.json"}
        self.members = {}
        self.remaining = byte_budget
        for ref in self.root["memberManifests"]:
            key = ref["objectKey"]
            if key != "members.json":
                raise ValueError("Unsupported capture manifest key")
            self.metadata.add(key)
            for member in json.loads((self.directory / key).read_text())["members"]:
                if member["objectKey"] in self.members:
                    raise ValueError("Repeated capture member")
                self.members[member["objectKey"]] = member

    def keys(self):
        return sorted(self.metadata | self.members.keys())

    def open(self, object_key: str):
        key = object_key
        if key in self.metadata:
            path = self.directory / key
            size = path.stat().st_size
            if size > 1024 * 1024:
                raise ValueError("Capture metadata exceeds bound")
            data = path.read_bytes()
        else:
            member = self.members[key]
            digest = member["sha256"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError("Invalid object digest")
            if type(member["byteSize"]) is not int or not 0 <= member["byteSize"] <= 12 * 1024 * 1024:
                raise ValueError("Capture member exceeds bound")
            path = self.corpus / "source-evidence/blobs/sha256" / digest[7:]
            if path.exists():
                with path.open("rb") as stream:
                    data = stream.read(member["byteSize"] + 1)
            else:
                with gzip.open(path.with_suffix(".gz"), "rb") as stream:
                    data = stream.read(member["byteSize"] + 1)
            if len(data) != member["byteSize"] or "sha256:" + hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("Retained object differs from declared bytes")
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ValueError("Retained read budget exceeded")
        return io.BytesIO(data)

    def verify(self):
        return admit_artifact(self, expected_pin=ArtifactPin(self.root["logicalId"], self.digest))

    def journal(self):
        with self.open("journal.jsonl") as stream:
            return [json.loads(line) for line in stream if line.strip()]

    def body(self, event: dict) -> bytes:
        key = event.get("blob_member")
        if key is None:
            matches = [name for name, member in self.members.items()
                       if member["sha256"] == event["sha256"] and member["byteSize"] == event["byte_size"]]
            if len(matches) != 1:
                raise ValueError("Legacy capture requires one exact digest/size member")
            key = matches[0]
        member = self.members[key]
        if member["sha256"] != event["sha256"] or member["byteSize"] != event["byte_size"]:
            raise ValueError("Journal and member capture pins differ")
        with self.open(key) as stream:
            return stream.read()


def compare_rows(expected: list[dict], actual: list[dict]) -> RowComparison:
    """Full value and duplicate-multiplicity equality; row order is separate."""
    def encoded(rows):
        return [json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) for row in rows]
    left, right = encoded(expected), encoded(actual)
    missing, added = Counter(left) - Counter(right), Counter(right) - Counter(left)
    return {"expected_rows": len(left), "actual_rows": len(right),
            "missing_row_occurrences": sum(missing.values()), "added_row_occurrences": sum(added.values()),
            "all_values_and_multiplicity_equal": not missing and not added,
            "ordered_rows_equal": left == right}


def code_pins() -> dict:
    from importlib import import_module
    from importlib.metadata import version
    from spicy_regs.generations import source_digest
    modules = [import_module(name) for name in (
        "spicy_docs.sources.gao.recommendations", "spicy_docs.schemas.gao_recommendation_tables",
        "spicy_docs.schemas.tables", "spicy_regs.etl_receipts",
        "spicy_regs.transforms.build_gao_recommendations",
        "spicy_regs.transforms.government_receipts", "spicy_regs.transforms.government_source_shapes")]
    files = [Path(__file__), Path(__file__).parents[1] / "uv.lock"]
    files += [_module_path(module) for module in modules]
    package_code = {}
    for name in ("spicy_regs", "spicy_docs", "rulespec_artifacts"):
        directory = _module_path(import_module(name)).parent
        package_code[name] = {"path": str(directory),
                              "source_sha256": source_digest(directory, (".py", ".json", ".xsd"))}
    return {"files": [pin(path) for path in files], "package_code": package_code, "packages": {
        name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", "pyarrow", "duckdb")}}


def selection(corpus: Path) -> dict:
    """Resolve journals without reading publisher payloads or running producers."""
    catalog = json.loads((corpus / "catalog.json").read_text())
    family = catalog["families"]["gao-recommendations"]
    generation = family["current_generation"]
    root_path = corpus / "lineage/generations/gao-recommendations" / generation[7:] / "artifact.json"
    root = json.loads(root_path.read_text())
    selected_sources = [ref["artifactDigest"] for ref in root["inputs"] if ref["role"] == "source-evidence"]
    if len(selected_sources) != 1:
        raise ValueError("Selected GAO generation requires one source artifact")
    captures = []
    for digest in family["source_artifacts"]:
        source = RetainedArtifact(corpus, digest)
        events = source.journal()
        export = [e for e in events if e.get("event") == "capture" and e.get("stage") == "gao-recommendations-export"]
        fold = [e for e in events if e.get("event") == "fold" and e.get("stage") == "gao-recommendations-export"]
        if len(export) != 1 or len(fold) != 1:
            raise ValueError("Capture requires one complete export and fold observation")
        capture = export[0]
        legacy = "capture_id" not in capture and "evidence_policy" not in capture
        if (capture.get("status_code") != 200 or capture.get("response_complete") is False
                or (capture.get("response_complete") is not True and not legacy)):
            raise ValueError("Export capture did not succeed completely")
        outputs = [e for e in events if e.get("event") == "retained-file" and e.get("dataset") == "gao_recommendations"]
        captures.append({"artifact": digest, "export": export[0], "fold": fold[0],
                         "expected_outputs": outputs,
                         "metadata_pins": [pin(source.directory / key) for key in sorted(source.metadata)]})
    captures.sort(key=lambda item: (item["export"]["observed_at"], item["artifact"]))
    if not captures or captures[-1]["artifact"] != selected_sources[0]:
        raise ValueError("Selection contains captures beyond or outside the selected generation")
    if len(captures[-1]["expected_outputs"]) != 1:
        raise ValueError("Selected output requires exactly one retained producer output")
    return {"format": "source-catalog-f-gao-selection/1", "family": "gao-recommendations",
            "corpus": str(corpus.resolve()), "selected_generation": generation,
            "selected_source_artifact": selected_sources[0], "catalog_pin": pin(corpus / "catalog.json"),
            "generation_root_pin": pin(root_path), "captures": captures, "code_pins": code_pins(),
            "coverage": "Explicit retained export history only; no live reads, no baseline injection"}


def _replay(manifest_path: Path, output: Path) -> dict:
    from spicy_docs.schemas.gao_recommendation_tables import GAO_RECOMMENDATIONS, shape_gao_recommendation
    from spicy_docs.sources.gao.recommendations import parse_recommendations_export
    from spicy_regs.transforms.build_gao_recommendations import fold
    from spicy_regs.transforms.government_receipts import migrate_outputs, POLICIES
    from spicy_regs.etl_receipts import read_with_receipts

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "source-catalog-f-gao-selection/1":
        raise ValueError("Unsupported F selection")
    corpus = Path(manifest["corpus"])
    frozen = manifest["code_pins"]
    if code_pins() != frozen:
        raise ValueError("Code/package selection changed")
    for item in [manifest["catalog_pin"], manifest["generation_root_pin"]]:
        assert_pin(Path(item["path"]), item)
    if selection(corpus) != manifest:
        raise ValueError("Selection differs from exact canonical journals and target")
    output.mkdir(parents=True, exist_ok=False)
    (output / "selection.json").write_bytes(manifest_path.read_bytes())
    history, steps = [], []
    selected_expected = None
    for item in manifest["captures"]:
        source = RetainedArtifact(corpus, item["artifact"])
        source.verify()
        export = parse_recommendations_export(source.body(item["export"]))
        before = len(history)
        fresh = [shape_gao_recommendation(rec, status_as_of=export.status_as_of, as_of=export.as_of)
                 for rec in export.recommendations]
        history, retirement = fold(history, fresh)
        step = {"artifact": item["artifact"], "export_sha256": item["export"]["sha256"],
                "status_as_of": export.status_as_of, "source_rows": len(fresh), "prior_rows": before,
                "recorded_prior_rows": item["fold"]["prior_rows"], "retirement": retirement,
                "transport_completeness_flag": item["export"].get("response_complete"),
                "payload_validation": "maintained whole-export parser and declared member digest/size"}
        step["recorded_prior_population_matches"] = before == item["fold"]["prior_rows"]
        for expected_output in item["expected_outputs"]:
            expected = pq.read_table(io.BytesIO(source.body(expected_output))).to_pylist()
            step["output_comparison"] = compare_rows(expected, history)
            if item["artifact"] == manifest["selected_source_artifact"]:
                selected_expected = expected
        steps.append(step)
        with (output / "progress.jsonl").open("a") as stream:
            stream.write(json.dumps(step, sort_keys=True) + "\n")
    if selected_expected is None:
        raise ValueError("Selected expected output absent")
    schema = pa.schema([(column, pa.string()) for column in GAO_RECOMMENDATIONS.columns])
    candidate = output / "gao_recommendations.parquet"
    pq.write_table(pa.Table.from_pylist(history, schema=schema), candidate, compression="zstd")
    final_comparison = compare_rows(selected_expected, history)
    migration = migrate_outputs((candidate,))
    restored = [row["raw_record"] for row in read_with_receipts(
        [candidate], [Path(migration["receipt_path"])], POLICIES["gao_recommendations"],
        generation_id=migration["generation_id"])]
    roundtrip = compare_rows(history, restored)
    for item in manifest["captures"]:
        for selected_pin in item["metadata_pins"]:
            assert_pin(Path(selected_pin["path"]), selected_pin)
    for item in [manifest["catalog_pin"], manifest["generation_root_pin"]]:
        assert_pin(Path(item["path"]), item)
    if code_pins() != frozen:
        raise ValueError("Code/package selection changed during replay")
    passed = (final_comparison["all_values_and_multiplicity_equal"]
              and roundtrip["all_values_and_multiplicity_equal"]
              and all(step["recorded_prior_population_matches"] for step in steps)
              and all(step.get("output_comparison", {}).get("all_values_and_multiplicity_equal", True) for step in steps))
    result = {"state": "qualified" if passed else "failed-source-only-recovery", "task": "T15",
              "family": "gao-recommendations", "selected_generation": manifest["selected_generation"],
              "steps": steps, "final_comparison": final_comparison, "native_receipt_roundtrip": roundtrip,
              "outputs": [pin(candidate), pin(Path(migration["receipt_path"]))], "code_pins": frozen,
              "scope": "Complete retained selected producer values and duplicate multiplicity; native candidate/receipt roundtrip",
              "limits": ["Historical native receipt IDs are not recreated", "No publisher completeness or other family proof",
                         "Baseline retirement requires reference migration and H acceptance"],
              "production_actions": []}
    (output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def replay(manifest_path: Path, output: Path) -> dict:
    """Preserve a terminal error for a newly created failed candidate only."""
    existed = output.exists()
    try:
        return _replay(manifest_path, output)
    except Exception as error:
        if not existed and output.is_dir() and not (output / "qualification.json").exists():
            failure = {"state": "error", "family": "gao-recommendations", "task": "T15",
                       "error_type": type(error).__name__, "selection_pin": pin(manifest_path),
                       "progress": str(output / "progress.jsonl"), "production_actions": [],
                       "scope": "Incomplete replay; no source-only qualification"}
            with (output / "qualification.json").open("x") as stream:
                stream.write(json.dumps(failure, indent=2) + "\n")
        raise


COURT_DELTA_ARTIFACT = "sha256:43757a506ac03cfc68d02d9edcf1ff101ab92a5ad5e696a9fd74ef81e4d7670a"


def court_code_pins() -> dict:
    from importlib import import_module
    result = code_pins()
    result["files"] += [pin(_module_path(import_module(name))) for name in (
        "spicy_docs.sources.courtlistener.search", "spicy_docs.reading.paged_json",
        "spicy_regs.sources.courtlistener", "spicy_regs.transforms.build_courtlistener",
        "spicy_regs.court_receipts", "spicy_regs.court_subjects")]
    return result


def court_selection(corpus: Path, artifact: str = COURT_DELTA_ARTIFACT) -> dict:
    """Pin selected query groups and every occurrence using metadata only."""
    from spicy_docs.sources.courtlistener.search import COURTLISTENER
    from urllib.parse import parse_qs, urlsplit
    source = RetainedArtifact(corpus, artifact)
    groups, ids = [], set()
    events = source.journal()
    for ordinal, event in enumerate(events):
        if event.get("event") == "selection" and event.get("stage") == "courtlistener":
            url = COURTLISTENER.check_url(event["url"])
            if event.get("max_records") is not None or parse_qs(urlsplit(url).query).get("type") != ["r"]:
                raise ValueError("Court delta requires uncapped RECAP search groups")
            groups.append({"selection_ordinal": ordinal, "selection": event, "captures": []})
        elif event.get("event") == "capture":
            if (not groups or event.get("stage") != "courtlistener-response"
                    or event.get("method") != "GET" or event.get("request_body") is not None
                    or event.get("status_code") != 200 or event.get("response_complete") is not True
                    or event.get("body_retained") is not True or event.get("evidence_policy") != "full"
                    or event.get("content_encoding") != "identity"
                    or event.get("resolved_url") != event.get("requested_url")):
                raise ValueError("Court capture is not a complete retained GET observation")
            COURTLISTENER.check_url(event["requested_url"])
            identity = event.get("capture_id")
            if not isinstance(identity, str) or not identity or identity in ids:
                raise ValueError("Court capture occurrence needs a distinct identity")
            ids.add(identity)
            member = source.members[event["blob_member"]]
            if member["sha256"] != event["sha256"] or member["byteSize"] != event["byte_size"]:
                raise ValueError("Court capture and body member pins differ")
            groups[-1]["captures"].append({"journal_ordinal": ordinal, "event": event})
    if not groups or any(not group["captures"] for group in groups):
        raise ValueError("Court selection has an empty or missing capture group")
    if any(group["captures"][0]["event"]["requested_url"] != group["selection"]["url"] for group in groups):
        raise ValueError("Court selection start differs from its first capture")
    if not any(event.get("event") == "build-outcome" and event.get("outcome") == "build-complete" for event in events):
        raise ValueError("Court acquisition lacks completed build observation")
    captures = [c["event"] for group in groups for c in group["captures"]]
    return {"format": "source-catalog-f-court-delta/1", "corpus": str(corpus.resolve()),
            "artifact": artifact, "groups": groups,
            "metadata_pins": [pin(source.directory / key) for key in sorted(source.metadata)],
            "journal_member": source.members["journal.jsonl"],
            "capture_occurrences": len(captures), "capture_occurrence_bytes": sum(c["byte_size"] for c in captures),
            "unique_body_bytes": sum(source.members[key]["byteSize"] for key in {c["blob_member"] for c in captures}),
            "code_pins": court_code_pins(), "scope": "Captured search delta only; no prior input or historical merge"}


def _court_replay(manifest_path: Path, output: Path) -> dict:
    import httpx
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.courtlistener.search import CourtListenerSearchReader, is_exact_count
    from spicy_regs.transforms.build_courtlistener import _shape
    from spicy_regs.court_receipts import write_court_rows, read_court_rows, local_receipt_selection, POLICIES
    from spicy_regs.court_subjects import normalize_court_row
    from spicy_regs.etl_receipts import ReceiptContext, split_record, read_attempts

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "source-catalog-f-court-delta/1":
        raise ValueError("Unsupported Court delta selection")
    corpus = Path(manifest["corpus"])
    if court_selection(corpus, manifest["artifact"]) != manifest:
        raise ValueError("Court selection/code differs from exact retained journals")
    output.mkdir(parents=True, exist_ok=False)
    (output / "selection.json").write_bytes(manifest_path.read_bytes())
    source = RetainedArtifact(corpus, manifest["artifact"], byte_budget=4_000_000)
    source.verify()
    results: list[CourtGroupResult] = []
    for group_index, group in enumerate(manifest["groups"]):
        consumed, shaped, occurrences, seen = [], [], [], set()
        def held_response(request):
            ordinal = len(consumed)
            if ordinal >= len(group["captures"]):
                raise ValueError("Court replay requested an uncaptured continuation")
            capture = group["captures"][ordinal]
            event = capture["event"]
            if (request.method != event["method"] or str(request.url) != event["requested_url"]
                    or request.content or "authorization" in request.headers):
                raise ValueError("Court replay request differs from selected occurrence")
            consumed.append(capture)
            return httpx.Response(200, stream=httpx.ByteStream(source.body(event)),
                                  headers={"content-type": event["content_type"]}, request=request,
                                  extensions={"spicy_docs_retained_observed_at": event["observed_at"]})
        class HeldTransport(httpx.BaseTransport):
            def replay_response(self, request):
                return held_response(request)

            def handle_request(self, request):
                raise ValueError("Court retained qualification cannot dispatch live requests")

        with CourtListenerSearchReader(budget=PagedJsonBudget(1, 12 * 1024 * 1024, 30, 0),
                                       transport=HeldTransport()) as reader:
            for page_index, page in enumerate(reader.search(group["selection"]["url"], max_pages=len(group["captures"]))):
                if len(consumed) != page_index + 1:
                    raise ValueError("Court replay page/capture occurrence differs")
                if page.next_url is not None and not page.records:
                    raise ValueError("Court continuation follows an empty page")
                capture = consumed[-1]
                for position, raw in enumerate(page.records):
                    # Match the high-level CourtListenerReader identity guard;
                    # the maintained page reader owns count/cursor validation.
                    identity = raw.get("docket_id")
                    if type(identity) is not int or identity <= 0 or identity in seen:
                        raise ValueError("Court search has missing, invalid or repeated docket identity")
                    seen.add(identity)
                    observation = {"artifact": manifest["artifact"], "capture_id": capture["event"]["capture_id"],
                                   "journal_ordinal": capture["journal_ordinal"], "result_position": position,
                                   "group_index": group_index, "page_index": page_index}
                    shaped.append({**_shape(dict(raw)), "source_observation": observation})
                    occurrences.append(dict(observation))
                if page.next_url is None and is_exact_count("r", page.declared_count) and len(seen) != page.declared_count:
                    raise ValueError("Court terminal distinct count differs")
        if len(consumed) != len(group["captures"]):
            raise ValueError("Court replay left selected capture occurrences unused")
        directory = output / f"group-{group_index:03d}"
        generation = "court-delta-" + hashlib.sha256((manifest["artifact"] + str(group_index)).encode()).hexdigest()
        witnesses = [{"source_id": c["event"]["capture_id"], "source_uri": c["event"]["requested_url"],
                      "sha256": c["event"]["sha256"], "locator": str(c["journal_ordinal"]), "body_version": None}
                     for c in consumed]
        context = ReceiptContext(generation, "expected", "court-delta", witnesses, {})
        accepted, refused = [], {}
        for ordinal, row in enumerate(shaped):
            try:
                normalized = normalize_court_row("court_dockets", row)
                split_record(POLICIES["court_dockets"], normalized, context)
                accepted.append(normalized)
            except (ValueError, TypeError, KeyError, pa.ArrowException):
                refused[f"court_dockets:delta:{ordinal}"] = row
        subject = write_court_rows("court_dockets", shaped, directory, witnesses=witnesses,
                                   generation_id=generation, attempt_prefix="delta", diagnostics={"group_index": group_index})
        receipts, receipt_generation = local_receipt_selection(subject)
        restored = list(read_court_rows(subject, dataset="court_dockets"))
        comparison = compare_rows(accepted, restored)
        attempts = list(read_attempts([receipts], POLICIES["court_dockets"], generation_id=receipt_generation))
        actual_refused = {a["attempt_id"]: a["processing_fields"]["raw_source_record"]
                          for a in attempts if a["outcome"] == "refused"}
        by_attempt = {a["attempt_id"]: a for a in attempts}
        for ordinal, observation in enumerate(occurrences):
            attempt_id = f"court_dockets:delta:{ordinal}"
            attempt = by_attempt.get(attempt_id)
            observation.update({"attempt_id": attempt_id, "receipt_id": attempt["receipt_id"] if attempt else None,
                                "outcome": attempt["outcome"] if attempt else "missing"})
        outcome_ok = (actual_refused == refused and len(attempts) == len(shaped)
                      and {a["attempt_id"] for a in attempts} == {f"court_dockets:delta:{i}" for i in range(len(shaped))}
                      and all(a["outcome"] in {"accepted", "refused"} for a in attempts))
        step: CourtGroupResult = {"group_index": group_index, "capture_ids": [c["event"]["capture_id"] for c in consumed],
                "source_occurrences": len(shaped), "accepted": len(accepted), "refused": len(refused),
                "complete_empty_search": not shaped, "comparison": comparison, "all_outcomes_match": outcome_ok,
                "outputs": [pin(subject), pin(receipts)]}
        (directory / "occurrences.json").write_text(json.dumps(occurrences, indent=2) + "\n")
        results.append(step)
        with (output / "progress.jsonl").open("a") as stream:
            stream.write(json.dumps(step) + "\n")
    source.verify()
    if court_selection(corpus, manifest["artifact"]) != manifest:
        raise ValueError("Court selection/code changed during replay")
    passed = all(r["all_outcomes_match"] and r["comparison"]["ordered_rows_equal"] for r in results)
    result = {"state": "qualified" if passed else "failed-delta-roundtrip", "task": "T14",
              "family": "courtlistener-captured-delta", "artifact": manifest["artifact"], "groups": results,
              "capture_occurrences": sum(len(r["capture_ids"]) for r in results),
              "scope": "Exact held search delta parsing and new native subject/receipt roundtrip",
              "limits": ["No prior input, merge or full historical docket proof", "Original receipt identities not recreated",
                         "No bulk/API replacement, T14 completion, publication or baseline retirement"], "production_actions": []}
    (output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def court_replay(manifest_path: Path, output: Path) -> dict:
    existed = output.exists()
    try:
        return _court_replay(manifest_path, output)
    except Exception as error:
        if not existed and output.is_dir() and not (output / "qualification.json").exists():
            (output / "qualification.json").write_text(json.dumps({"state": "error", "task": "T14",
                "family": "courtlistener-captured-delta", "error_type": type(error).__name__,
                "selection_pin": pin(manifest_path), "production_actions": []}, indent=2) + "\n")
        raise


FCC_GENERATION = "sha256:7de59570ac98802f861d214ffa91b5a7d1fd3208c788c985a57ee9acb8880bbc"
FCC_READ_BUDGET = 256 * 1024 * 1024


def _fcc_metadata(path: Path, maximum: int = 1024 * 1024) -> bytes:
    size = path.stat().st_size
    if size > maximum:
        raise ValueError("FCC metadata exceeds pre-read bound")
    with path.open("rb") as stream:
        data = stream.read(size + 1)
    if len(data) != size:
        raise ValueError("FCC metadata changed during bounded read")
    return data


class FccRetainedArtifact(RetainedArtifact):
    """Reserve metadata and decoded reads before touching retained FCC bytes."""

    def __init__(self, corpus: Path, digest: str, *, byte_budget: int = FCC_READ_BUDGET):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("Expected artifact SHA-256")
        directory = corpus / "source-evidence" / digest[7:]
        sizes = [(directory / key).stat().st_size for key in ("artifact.json", "members.json")]
        metadata_bytes = sum(sizes)
        if any(size > 1024 * 1024 for size in sizes) or metadata_bytes + 2 > byte_budget:
            raise ValueError("FCC metadata exceeds pre-read budget")
        self.corpus, self.digest, self.directory = corpus, digest, directory
        self.root = json.loads(_fcc_metadata(directory / "artifact.json", sizes[0]))
        manifests = self.root["memberManifests"]
        if len(manifests) != 1 or manifests[0]["objectKey"] != "members.json":
            raise ValueError("FCC capture requires one fixed member manifest")
        self.metadata = {"artifact.json", "members.json"}
        self.members = {}
        for member in json.loads(_fcc_metadata(directory / "members.json", sizes[1]))["members"]:
            key = member["objectKey"]
            if key in self.members or key in self.metadata:
                raise ValueError("FCC repeated or shadowed capture member")
            self.members[key] = member
        self.remaining = byte_budget
        from rulespec_artifacts import expected_artifact_digest
        if self.root.get("artifactDigest") != digest or expected_artifact_digest(self.root) != digest:
            raise ValueError("FCC source metadata digest differs")
        self.remaining -= metadata_bytes
        self.read_allowance = byte_budget - metadata_bytes - 2
        self.reserved_read_bytes = metadata_bytes + 2

    def open(self, object_key: str):
        key = object_key
        size = ((self.directory / key).stat().st_size if key in self.metadata else self.members[key]["byteSize"])
        if type(size) is not int or size < 0 or size + 1 > self.read_allowance:
            raise ValueError("FCC decoded pre-read budget exceeded")
        # Include one-byte drift probes and conservatively charge failed reads.
        self.read_allowance -= size + 1
        self.reserved_read_bytes += size + 1
        if key in self.metadata:
            data = _fcc_metadata(self.directory / key, size)
            self.remaining -= len(data)
            return io.BytesIO(data)
        return super().open(key)


def fcc_code_pins() -> dict:
    from importlib import import_module
    result = code_pins()
    modules = ("spicy_docs.sources.fcc_ecfs", "spicy_docs.reading.paged_json",
               "spicy_regs.transforms.build_fcc_ecfs")
    result["files"] += [pin(_module_path(import_module(name))) for name in modules]
    return result


def fcc_selection(corpus: Path, generation: str = FCC_GENERATION) -> dict:
    """Select one captured dated refresh without opening responses or baseline tables."""
    from datetime import date, timedelta
    from urllib.parse import parse_qs, urlsplit
    from spicy_docs.sources.fcc_ecfs import proceedings_url

    if not re.fullmatch(r"sha256:[0-9a-f]{64}", generation):
        raise ValueError("Expected FCC generation SHA-256")
    directory = corpus / "lineage/generations/fcc-proceedings" / generation[7:]
    from rulespec_artifacts import expected_artifact_digest
    root = json.loads(_fcc_metadata(directory / "artifact.json"))
    if root["artifactDigest"] != generation or expected_artifact_digest(root) != generation:
        raise ValueError("FCC generation identity differs")
    sources = [ref["artifactDigest"] for ref in root["inputs"] if ref["role"] == "source-evidence"]
    if len(sources) != 1:
        raise ValueError("FCC refresh requires exactly one selected source artifact")
    source = FccRetainedArtifact(corpus, sources[0], byte_budget=FCC_READ_BUDGET)
    if source.root.get("spec", {}).get("family") != "fcc-proceedings":
        raise ValueError("FCC source family differs")
    events = source.journal()
    captures: list[CaptureOccurrence] = [{"journal_ordinal": i, "event": e} for i, e in enumerate(events) if e.get("event") == "capture"]
    if not captures:
        raise ValueError("FCC refresh has no captured requests")
    ids = set()
    for item in captures:
        event = item["event"]
        if (event.get("stage") != "fcc-proceedings-response" or event.get("method") != "GET"
                or type(event.get("status_code")) is not int or event["status_code"] != 200
                or event.get("response_complete") is not True or event.get("body_retained") is not True
                or event.get("request_body") is not None or not event.get("capture_id")
                or event["capture_id"] in ids or event.get("requested_url") != event.get("resolved_url")):
            raise ValueError("FCC capture is incomplete, repeated or outside the selected operation")
        ids.add(event["capture_id"])
        url = urlsplit(event["requested_url"])
        if url.scheme != "https" or url.netloc != "publicapi.fcc.gov" or url.path != "/ecfs/proceedings" or url.fragment:
            raise ValueError("FCC capture endpoint differs")
        query = parse_qs(url.query, strict_parsing=True)
        if set(query) != {"date_proceeding_created", "sort", "limit", "offset"} or any(len(v) != 1 for v in query.values()):
            raise ValueError("FCC capture has undeclared query scope")
        member = source.members.get(event.get("blob_member"))
        if (member is None or member.get("role") != "source-body" or member["sha256"] != event["sha256"]
                or member["byteSize"] != event["byte_size"]):
            raise ValueError("FCC capture/member pins differ")
    first = captures[0]["event"]
    query = parse_qs(urlsplit(first["requested_url"]).query)
    window = re.fullmatch(r"\[gte\](\d{4}-\d{2}-\d{2})\[lte\](\d{4}-\d{2}-\d{2})", query["date_proceeding_created"][0])
    if window is None or query["limit"] != ["250"]:
        raise ValueError("FCC refresh requires the maintained 250-record initial window")
    since, publisher_bound = window.groups()
    until = (date.fromisoformat(publisher_bound) - timedelta(days=1)).isoformat()
    if since != "1990-01-01" or proceedings_url(created_from=since, created_to=until, limit=250, descending=False) != first["requested_url"]:
        raise ValueError("FCC initial query differs from the complete dated refresh")
    selections = [e for e in events if e.get("event") == "selection"]
    decisions = [e for e in events if e.get("event") == "proceeding-selection"]
    outputs = [e for e in events if e.get("event") == "retained-file"]
    outcomes = [e for e in events if e.get("event") == "build-outcome"]
    if (len(selections) != 1 or selections[0].get("stage") != "fcc-proceedings" or selections[0].get("since") != since
            or len(decisions) != 1 or decisions[0].get("selection_rule") != "earliest-created/latest-modified/most-populated/lowest-id"
            or len(outputs) != 1 or outputs[0].get("dataset") != "fcc_proceedings"
            or outputs[0].get("name") != "fcc_proceedings.parquet" or outputs[0].get("stage") != "government-producer-output"
            or outputs[0].get("body_retained") is not True or outputs[0].get("response_complete") is not True
            or len(outcomes) != 1 or outcomes[0].get("outcome") != "build-complete"):
        raise ValueError("FCC refresh selection, decisions or producer output absent")
    output_member = source.members.get(outputs[0].get("blob_member"))
    if (output_member is None or output_member.get("role") != "source-body"
            or output_member["sha256"] != outputs[0]["sha256"] or output_member["byteSize"] != outputs[0]["byte_size"]):
        raise ValueError("FCC producer-output/member pins differ")
    return {"format": "source-catalog-f-fcc-refresh/1", "family": "fcc-proceedings", "corpus": str(corpus.resolve()),
            "selected_generation": generation, "artifact": sources[0], "captures": captures,
            "since": since, "until": until, "publisher_upper_bound": publisher_bound,
            "producer_decision": decisions[0], "producer_output": outputs[0],
            "recorded_packages": next(e["packages"] for e in events if e.get("event") == "run"),
            "metadata_pins": [pin(directory / "artifact.json")] + [pin(source.directory / k) for k in sorted(source.metadata)],
            "code_pins": fcc_code_pins(), "decoded_source_read_cap": FCC_READ_BUDGET,
            "scope": "Selected dated producer refresh and candidate native raw-record roundtrip only",
            "limits": ["Historical baseline acquisition absent", "Historical native receipt inheritance not replayed",
                       "No undated/full publisher population closure", "Recorded package compatibility requires exact replay evidence"]}


def _fcc_rows(documents: list[dict]) -> tuple[list[dict], dict]:
    """The maintained producer's rows and selection decision for a retained whole walk."""
    from spicy_regs.transforms.build_fcc_ecfs import PROCEEDING_SELECTION_RULE, proceeding_rows
    if not documents:
        raise ValueError("FCC empty whole refresh refuses")
    rows, nameless, repeated = proceeding_rows(documents)
    return rows, {"nameless_ids": nameless, "repeated_names": repeated, "selection_rule": PROCEEDING_SELECTION_RULE}


def _fcc_replay(manifest_path: Path, output: Path, owned: list) -> dict:
    from datetime import date
    import httpx
    from spicy_regs.transforms.build_fcc_ecfs import _fetch_fcc, _PROCEEDING_SCHEMA
    from spicy_regs.transforms.government_receipts import migrate_outputs, POLICIES, _INHERITED
    from spicy_regs.etl_receipts import read_with_receipts

    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "source-catalog-f-fcc-refresh/1":
        raise ValueError("Unsupported FCC refresh selection")
    corpus = Path(manifest["corpus"])
    if fcc_selection(corpus, manifest["selected_generation"]) != manifest:
        raise ValueError("FCC selection/code differs from exact retained journals")
    if _INHERITED.get():
        raise ValueError("FCC refresh candidate must not inherit external receipt state")
    output.mkdir(parents=True, exist_ok=False)
    owned.append(True)
    (output / "selection.json").write_bytes(manifest_path.read_bytes())
    source = FccRetainedArtifact(corpus, manifest["artifact"], byte_budget=FCC_READ_BUDGET)
    source.verify()
    consumed = []
    def held_response(request):
        ordinal = len(consumed)
        if ordinal >= len(manifest["captures"]):
            raise ValueError("FCC replay requested an uncaptured continuation")
        capture = manifest["captures"][ordinal]
        event = capture["event"]
        if request.method != event["method"] or str(request.url) != event["requested_url"] or request.content:
            raise ValueError("FCC replay request differs from selected occurrence")
        consumed.append(capture)
        return httpx.Response(event["status_code"], stream=httpx.ByteStream(source.body(event)),
                              headers={"content-type": event["content_type"]}, request=request)
    documents = list(_fetch_fcc("proceedings", since=date.fromisoformat(manifest["since"]),
                               until=date.fromisoformat(manifest["until"]), api_key="offline-fixture",
                               transport=httpx.MockTransport(held_response)))
    if len(consumed) != len(manifest["captures"]):
        raise ValueError("FCC replay left selected capture occurrences unused")
    rows, decision = _fcc_rows(documents)
    if any(decision[key] != manifest["producer_decision"].get(key) for key in decision):
        raise ValueError("FCC duplicate-document or nameless selection differs")
    expected = pq.read_table(io.BytesIO(source.body(manifest["producer_output"]))).to_pylist()
    comparison = compare_rows(expected, rows)
    candidate = output / "fcc_proceedings.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_PROCEEDING_SCHEMA), candidate, compression="zstd")
    migration = migrate_outputs((candidate,))
    restored = [row["raw_record"] for row in read_with_receipts(
        [candidate], [Path(migration["receipt_path"])], POLICIES["fcc_proceedings"], generation_id=migration["generation_id"])]
    roundtrip = compare_rows(rows, restored)
    source.verify()
    if fcc_selection(corpus, manifest["selected_generation"]) != manifest:
        raise ValueError("FCC selection/code changed during replay")
    passed = comparison["ordered_rows_equal"] and roundtrip["ordered_rows_equal"]
    result = {"state": "qualified" if passed else "failed-source-only-recovery", "family": "fcc-proceedings",
              "scope": manifest["scope"], "selected_generation": manifest["selected_generation"],
              "artifact": manifest["artifact"], "logical_until": manifest["until"], "publisher_upper_bound": manifest["publisher_upper_bound"],
              "capture_occurrences": [{"journal_ordinal": c["journal_ordinal"], "capture_id": c["event"]["capture_id"]} for c in consumed],
              "source_documents": len(documents), "producer_decision": decision, "producer_comparison": comparison,
              "native_candidate_raw_roundtrip": roundtrip, "outputs": [pin(candidate), pin(Path(migration["receipt_path"]))],
              "decoded_source_successful_bytes": FCC_READ_BUDGET - source.remaining,
              "decoded_source_reserved_bytes": source.reserved_read_bytes,
              "code_pins": manifest["code_pins"], "limits": manifest["limits"], "production_actions": []}
    (output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def fcc_replay(manifest_path: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    owned = []
    try:
        return _fcc_replay(manifest_path, output, owned)
    except Exception as error:
        if owned:
            (output / "qualification.json").write_text(json.dumps({"state": "error", "family": "fcc-proceedings",
                "error_type": type(error).__name__, "message": str(error), "production_actions": []}, indent=2) + "\n")
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select-gao")
    select.add_argument("--corpus", type=Path, required=True)
    select.add_argument("--manifest", type=Path, required=True)
    run = commands.add_parser("replay-gao")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    court_select = commands.add_parser("select-court-delta")
    court_select.add_argument("--corpus", type=Path, required=True)
    court_select.add_argument("--artifact", default=COURT_DELTA_ARTIFACT)
    court_select.add_argument("--manifest", type=Path, required=True)
    court_run = commands.add_parser("replay-court-delta")
    court_run.add_argument("--manifest", type=Path, required=True)
    court_run.add_argument("--output", type=Path, required=True)
    fcc_select = commands.add_parser("select-fcc-refresh")
    fcc_select.add_argument("--corpus", type=Path, required=True)
    fcc_select.add_argument("--generation", default=FCC_GENERATION)
    fcc_select.add_argument("--manifest", type=Path, required=True)
    fcc_run = commands.add_parser("replay-fcc-refresh")
    fcc_run.add_argument("--manifest", type=Path, required=True)
    fcc_run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "select-fcc-refresh":
        with args.manifest.open("x") as stream:
            stream.write(json.dumps(fcc_selection(args.corpus, args.generation), indent=2) + "\n")
    elif args.command in {"select-gao", "select-court-delta"}:
        value = selection(args.corpus) if args.command == "select-gao" else court_selection(args.corpus, args.artifact)
        with args.manifest.open("x") as stream:
            stream.write(json.dumps(value, indent=2) + "\n")
    else:
        runners = {"replay-gao": replay, "replay-court-delta": court_replay, "replay-fcc-refresh": fcc_replay}
        value = runners[args.command](args.manifest, args.output)
        print(json.dumps({"state": value["state"], "family": value["family"]}))
        if value["state"] != "qualified":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
