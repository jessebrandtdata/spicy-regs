"""Select source-owned page readers and validate their distinct row coordinates.

Custody, checkpoints and generation writing stay in the shared page pipeline.
These adapters preserve each reader's output without inventing ledger fields
for financial summaries or mailing tables.
"""

from importlib import import_module

PROFILES = ("ledger", "financial-summary", "mailing-summary", "allowance-summary", "allowance-formula")
READERS = {
    ("house", "ledger"): ("house_disbursements_pdf", "iter_house_disbursements_pages"),
    ("senate", "ledger"): ("senate_disbursements", "iter_senate_disbursements_pages"),
    ("house", "financial-summary"): ("house_financial_summaries", "iter_house_summary_pages"),
    ("senate", "financial-summary"): ("senate_disbursement_summaries", "iter_senate_summary_pages"),
    ("house", "mailing-summary"): ("house_mailing_summaries", "iter_house_mailing_summary_pages"),
    ("house", "allowance-summary"): ("house_allowance_summaries", "iter_house_allowance_summary_pages"),
    ("house", "allowance-formula"): ("house_allowance_formulas", "iter_house_allowance_formula_pages"),
}


def reader_module(chamber, profile):
    try:
        return "spicy_docs.reading." + READERS[chamber, profile][0]
    except KeyError as error:
        raise ValueError("No source-owned page reader for this chamber/profile") from error


def read_source_pages(source, body, profile):
    from datetime import date

    module = import_module(reader_module(source["chamber"], profile))
    reader = getattr(module, READERS[source["chamber"], profile][1])
    options = {}
    if "reading_pages" in source:
        options["pages"] = source["reading_pages"]
    if profile == "ledger" or (source["chamber"], profile) == ("house", "financial-summary"):
        options.update(
            report_period_start=date.fromisoformat(source["period_start"]),
            report_period_end=date.fromisoformat(source["period_end"]),
        )
    if source.get("structured_observations"):
        if (source["chamber"], profile) not in {("senate", "financial-summary"), ("house", "ledger")}:
            raise ValueError("This source/profile has no retained structured observation reader")
        return _structured_pages(source, body, reader, profile, options)
    return reader(body, **options)


def _structured_pages(source, body, native_reader, profile, options):
    """Replay selected observations; no backend, new rendering or inferred boxes."""
    import hashlib
    from spicy_docs.extraction import DefaultReader
    from spicy_regs.pipelines.congressional_disbursement_pages import processing_inputs

    processing_inputs(source)
    if (
        len(body) != source["source_byte_size"]
        or "sha256:" + hashlib.sha256(body).hexdigest() != source["source_sha256"]
    ):
        raise ValueError("Structured page original differs from its selected bytes")
    with DefaultReader().open(body, "application/pdf") as document:
        if document.page_count != source["physical_page_count"]:
            raise ValueError("Structured page original physical page count differs")
    selected = {item["page"]: item for item in source["structured_observations"]}
    native_pages = [page for page in source["reading_pages"] if page not in selected]
    native = native_reader(body, **{**options, "pages": native_pages}) if native_pages else iter(())
    try:
        for page in source["reading_pages"]:
            if page not in selected:
                yield next(native)
                continue
            yield read_retained_observation(selected[page], chamber=source["chamber"], profile=profile)
    finally:
        close_native = getattr(native, "close", None)
        if callable(close_native):
            close_native()


def read_retained_summary_observation(item):
    """One bounded loader for initial reading and exact typed-page reconstruction."""
    import json
    from PIL import Image
    from spicy_docs.extraction import Raster
    from spicy_regs.pipelines.congressional_disbursement_pages import verify_pin

    image_path = verify_pin(item["image"])
    if image_path.stat().st_size > 16 * 1024**2 or item["result"]["bytes"] > 32 * 1024**2:
        raise ValueError("Retained structured observation exceeds the bounded page input size")
    with Image.open(image_path) as image:
        if image.format != "PNG" or image.width * image.height > 40_000_000:
            raise ValueError("Unsupported or oversized retained structured page raster")
        raster = Raster(image_path.read_bytes(), image.width, image.height)
    qualification = json.loads(verify_pin(item["qualification"]).read_text())
    module = import_module(reader_module("senate", "financial-summary"))
    return module.read_senate_summary_observation(
        verify_pin(item["result"]).read_bytes(), raster, qualification=qualification
    )


def read_retained_observation(item, *, chamber, profile):
    """Dispatch exact retained evidence through its source-owned interpretation."""
    if (chamber, profile) == ("senate", "financial-summary"):
        return read_retained_summary_observation(item)
    if (chamber, profile) != ("house", "ledger"):
        raise ValueError("No retained observation reader for this source/profile")
    import json
    from PIL import Image
    from spicy_docs.extraction import Raster
    from spicy_docs.reading.ocr_observations import retained_capture_observation
    from spicy_docs.reading.house_disbursements_observations import read_house_ledger_observation
    from spicy_regs.pipelines.congressional_disbursement_pages import verify_pin

    for key in ("result", "image", "page_record", "request", "response", "plan", "qualification"):
        if item[key]["bytes"] > (16 if key == "image" else 32) * 1024**2:
            raise ValueError("Retained House observation exceeds its bounded input size")
    image_path = verify_pin(item["image"])
    with Image.open(image_path) as image:
        if image.format != "PNG" or image.width * image.height > 40_000_000:
            raise ValueError("Unsupported or oversized retained House page raster")
        raster = Raster(image_path.read_bytes(), image.width, image.height)
    qualification = json.loads(verify_pin(item["qualification"]).read_text())
    outcome, configuration = retained_capture_observation(
        verify_pin(item["result"]).read_bytes(),
        raster,
        page_bytes=verify_pin(item["page_record"]).read_bytes(),
        request_gzip=verify_pin(item["request"]).read_bytes(),
        response_bytes=verify_pin(item["response"]).read_bytes(),
        plan_bytes=verify_pin(item["plan"]).read_bytes(),
        binding=item["capture_binding"],
    )
    return read_house_ledger_observation(
        outcome,
        binding=qualification["binding"],
        qualification=qualification,
        configuration=configuration,
    )


def record_kind(row):
    return row.get("row_kind") or row.get("table_kind") or "unresolved_summary_kind"


def validate_page_content(page, profile):
    """Check retained indices and row occurrence identity, without reinterpreting cells."""
    if profile == "allowance-formula":
        import_module(reader_module("house", profile)).validate_formula_page(page)
        return
    if page.get("reading_method") == "reviewed_structured_observation":
        if profile != "ledger":
            raise ValueError("Reviewed House ledger cannot enter another reading profile")
        module = import_module("spicy_docs.reading.house_disbursements_observations")
        module.validate_house_ledger_observation(page)
        return
    if page.get("reading_method") == "qualified_structured_observation":
        if profile != "financial-summary":
            raise ValueError("Structured summary cannot enter another reading profile")
        import_module(reader_module("senate", profile)).validate_senate_structured_summary(page)
        return
    if "words" in page:
        if [word["index"] for word in page["words"]] != list(range(len(page["words"]))):
            raise ValueError("PDF reading repeats or omits a native word index")
        if "lines" in page:
            indices = [index for line in page["lines"] for index in line["word_indices"]]
            indices.extend(page["outside_ledger_word_indices"])
            if sorted(indices) != list(range(len(page["words"]))):
                raise ValueError("PDF reading omitted or repeated a native word")
        for row in page["records"]:
            if any(type(i) is not int or not 0 <= i < len(page["words"]) for i in row["word_indices"]):
                raise ValueError("PDF financial row names a nonexistent original word")
        money = [i for row in page["records"] for i in row.get("money_word_indices", [])]
        if len(money) != len(set(money)):
            raise ValueError("PDF financial summaries repeat an original monetary token")
    elif profile not in {"mailing-summary", "allowance-summary"}:
        raise ValueError("PDF page profile requires retained original words")
    if profile in {"mailing-summary", "allowance-summary"}:
        coordinates = [(row["table"], row["row"]) for row in page["records"]]
        first_row = 0 if profile == "allowance-summary" else 1
        for table, row in coordinates:
            if not 0 <= table < len(page["tables"]) or not first_row <= row < page["tables"][table]["row_count"]:
                raise ValueError("Summary row lies outside its retained source table")
        if profile == "allowance-summary":
            validate = import_module(reader_module("house", profile)).validate_allowance_record
            for record in page["records"]:
                validate(page, record)
    elif profile == "financial-summary" and "grids" in page:
        coordinates = [(row["table"], row["ruled_row"], row["line"]) for row in page["records"]]
    else:
        coordinates = [(row["line"],) for row in page["records"]]
    if coordinates != sorted(set(coordinates)):
        raise ValueError("PDF reading repeats or reorders a financial row")
