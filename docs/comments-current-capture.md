# Capture original selected comments

`prepare_comments_selected_export.prepare` keeps the original selected input
pair and its byte pins. Its output receipts identify that pair and the original
receipt, identify the output row and processor, and retain actual outcomes and
diagnostics. They omit embedded source/conversion records and copied processing
history. Promoted reference and extraction facts stay in the comments table.
Both local and remote preparation use this same receipt shape.

The original pair is the input table for replay; preserve it. The smaller output
receipts are audit links, not a replacement input table. Exact-input readers
such as `materialize_internal` still require the original selected receipts and
refuse output receipts without a retained input. Querying comments,
building the index, and complete subject/receipt admission use the native
subject and its identity/version and do not require those embedded records.

Future catalog writes retain one `raw_source_record` and file metadata because
catalog replay and incremental processing actually consume that input. Both
bulk and row readers use the maintained catalog normalizer for flat inputs or
the same Regulations.gov projection for API inputs, and check that it reproduces
the selected subject. They also read historical
`raw_conversion_inputs`. New writes omit the second normalized copy and do not
inherit earlier input payloads into diagnostics. When rebinding older receipts,
the normalized copy is removed only after exact per-row equality proves it can
be reproduced from the original; an unproven custom conversion remains explicit.
Retirement receipts keep the actual retirement reason and predecessor identity
without embedding another copy of the input history.

The capture command (`scripts/capture_comments_current.py`, with its test) was
removed after the one-shot v1 promotion it fed; comments now publish from the
catalog table alone. Git history keeps both. It took `EXPECTED.json`, which
selects the complete nullable string schema and exact table UUID, snapshot ID
and schema ID, and runtime pins.

The optional PyIceberg reader loads the REST table with its vended file credentials.
It plans the selected snapshot, checks actual live file/delete types, and reads
one planned file at a time through public `ArrowScan`. PyIceberg handles column
IDs, missing nullable columns, delete sequencing and position masks. Unsupported
formats or equality deletes produce a retained refusal. The source retains all
original columns, strings, nulls and repeated rows.

PyIceberg materializes each data file and its applicable delete data before
returning its batches. Feeding the planner's complete task list into that reader
can queue the whole table; this adapter supplies one task at a time. Client
batches and output row groups do not impose a byte limit on a file or record.
Run through a separately admitted whole-process RSS, time and disk watchdog;
refuse oversized files rather than increase limits during a run. Vended credentials
must remain usable for the admitted capture duration; expired credentials refuse
the attempt. Partial files remain private evidence.

The reader verifies fresh start/end identity and original field definitions,
then independently reads the complete exported file. Length-framed UTF-8 field
hashes check every value and ordinal from decoded reader to output, including
nulls and repeats. Physical file/delete counts are observations, not a logical
population. Local population and agency queries operate on the exported logical
rows. Only `RESULT.json` with `status: captured`, plus the successful external
resource result, qualifies the local input. This does not publish or convert it.

Each planned task now writes a separate part under `OUTPUT/parts/`. A task is
complete only after its reader and writer close and full local readback matches
the original schema, row count and field hashes. An atomic receipt pins the
part bytes to the selected snapshot, runtime, capture code, name mapping,
partition definitions and exact data/delete association. A fully deleted task
has a verified zero-row part and a completion receipt too.

Use `--max-tasks N` to bound newly read tasks in an admitted attempt. A result
of `checkpointed` records progress and leaves the full input unqualified. Resume
that same output with `--resume`, the same expected metadata and runtime, and
an appropriate new resource grant. Resume verifies every completed part, restores
the original pinned task order, and reads only unfinished tasks. Corrupted parts,
changed source identity or read settings, and changed task associations refuse
before reuse. Existing captured outputs are preserved; unfinished files without
task receipts cannot skip source work.
Use one owned executor per output directory; the coordinator grants each attempt.

`PROGRESS.json` and `attempts/*/progress.jsonl` retain task phases, elapsed time,
logical rows and output bytes. `attempts/*/PLANNING.json` measures compressed
file sizes and shared delete-file associations from metadata. Declared input
bytes are not observed network traffic. Shared-delete I/O costs remain a
hypothesis until a supervised run measures them.

When all tasks are complete, the reader streams local parts into the final
`comments.parquet` and performs the complete existing validation. All parts and
interrupted attempts remain within the caller's disk budget. A killed final
write can be rebuilt from verified parts; an already qualified local captured
result is not overwritten by resume. The separately supervised actual-source
delete oracle remains required by the full qualification driver.
