"""Narrow future H annual-code and D CFR commands for the maintained runner.

Activation/import closure, grants and accounting belong to operational_runner.
Source interpretation stays in maintained source code (the US Code editions
builder/auditor and cfr_native_audit). Accepted historical requests and
preparations remain unchanged.

These commands run in an offline payload. They therefore avoid the runner's
`durable` writer (its import-time os.open binding is the guarded one) and write
candidate files plainly; the owner hashes and seals them after closure.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from spicy_regs.operational_runner import pinned_bytes, read_json, require


def annual(config, phase, candidate, output):
    from spicy_regs.sources import uscode_editions as builder, uscode_editions_audit as auditor

    selection = pinned_bytes(config["selection"])

    def check():
        require(
            builder._code_pins() == auditor.reader_pins() == config["expected_reader_pins"], "Reader recipe differs"
        )
        generation = builder._identity(
            "uscode-editions-public-v2", builder._digest(selection), config["expected_reader_pins"]
        )
        require(generation == config["expected_generation"], "Selected generation differs")

    check()
    if phase == "h-build":
        result = builder.build(Path(config["selection"]["path"]), candidate)
        require(result["generation"] == config["expected_generation"], "Built generation differs")
    else:
        require(phase == "h-audit", "Annual phase differs")
        require(
            auditor.main(
                [
                    str(candidate),
                    "--selection",
                    config["selection"]["path"],
                    "--annual-year",
                    str(config["annual_year"]),
                    "--output",
                    str(output),
                ]
            )
            == 0,
            "Complete annual source audit failed",
        )
        result = read_json(output)
        expected = config["conservation"]
        require(
            result["verdict"] == "APPROVE" and result["generation"] == config["expected_generation"],
            "Audit verdict/generation",
        )
        for key, expected_key in (
            ("rows", "expected_rows"),
            ("content_categories", "expected_content_categories"),
            ("complete_original_members", "expected_complete_members"),
        ):
            require(result[key] == expected[expected_key], "Annual conservation differs: " + key)
        require(
            result["source_sha256"] == config["original_input"]["sha256"]
            and result["selection_sha256"] == config["selection"]["sha256"],
            "Audit source/selection differs",
        )
        # The candidate is exactly the builder's closed file set, nothing more or less.
        rebuild = read_json(candidate / "rebuild.json")
        names = {"selection.json", "rebuild.json", "progress.jsonl"}
        for table, rows in expected["expected_rows"].items():
            require(rebuild["outputs"][table]["rows"] == rows, "Rebuild counts differ: " + table)
            names.add(table + "/" + table + ".parquet")
        require(set(rebuild["outputs"]) == set(expected["expected_rows"]), "Rebuild output tables differ")
        actual = {path.relative_to(candidate).as_posix() for path in candidate.rglob("*") if path.is_file()}
        require(actual == names, "Candidate extra/missing files")
        require((candidate / "selection.json").read_bytes() == selection, "Candidate selection differs")
    check()


def cfr(config, phase, candidate, output):
    from spicy_regs import cfr_native_audit as compare

    pin = config["capture_config"]
    capture = json.loads(pinned_bytes(pin))
    compare.verify_custody(capture)
    backend = compare.maintained_backend()
    # Same maintained/parser-vs-independent traversal check as D's preflight.
    from spicy_docs.reading.xml_tree import read_xml_tree
    from spicy_docs.sources.govinfo.mods import parse_govinfo_mods

    tiny = b'<CFRDOC><X a="1">tiny<X/>tail</X></CFRDOC>'
    tree = read_xml_tree(
        tiny,
        expected_root="CFRDOC",
        error_type=ValueError,
        label="tinyCFR",
        max_bytes=1024,
        max_elements=20,
        max_depth=8,
    )
    require(compare.maintained_events(tree.element) == compare.independent_events(tiny), "CFR preflight differs")
    mods = b'<mods xmlns="http://www.loc.gov/mods/v3"><titleInfo><title>tiny</title></titleInfo></mods>'
    mapped = parse_govinfo_mods(mods, max_bytes=1024, max_elements=20)
    require(
        compare.maintained_events(mapped.package.element) == compare.independent_events(mods), "MODS preflight differs"
    )
    preflight = {"state": "tiny-maintained-preflight-passed", "capture_config": pin}
    if phase == "d-preflight":
        candidate.mkdir()
        with (candidate / "preflight.json").open("x") as stream:
            json.dump(preflight, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    else:
        require(phase == "d-semantic", "CFR phase differs")
        # This run's own sealed preflight is the only acceptance; one grant covers both phases.
        require(read_json(candidate / "preflight.json") == preflight, "CFR candidate selection differs")
        raw = compare.run(capture, backend)
        with output.open("xb") as stream:
            stream.write(raw)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("h-build", "h-audit", "d-preflight", "d-semantic"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    require(args.phase in ("h-build", "d-preflight") or args.output is not None, "Audit output required")
    raw = args.config.read_bytes()
    function = annual if args.phase.startswith("h-") else cfr
    function(json.loads(raw), args.phase, args.candidate, args.output)
    require(args.config.read_bytes() == raw, "Adapter config drift")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
