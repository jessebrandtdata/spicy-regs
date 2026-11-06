# Central raw archive and rebuildable catalog

SpicyRegs keeps civic and legal source originals and acquisition receipts in
`~/Work/corpora/spicyregs/raw`. The source catalog is a disposable SQLite index
built from that archive and the checked-in source registry. Building, querying
and exporting it never downloads data. Keep generated tables, processing
artifacts, models, software and catalog caches outside the archive.

RefSpec-only subject vocabularies, unrelated research captures and test/development
evidence live separately at `~/Work/corpora/non-spicyregs/raw`. Both roots retain
original bytes and receipts in the same native format and can be mirrored and
verified independently. Shared support evidence may be copied into both roots.
Saved civic/legal inputs remain in the SpicyRegs archive even when they have no
current consumer. The default catalog shows current source families and their
source-file inventory. Public details also offer **Files that need attention** for
scoped source responses that are missing, failed, incomplete or unchecked. Those
entries remain separate from the main file counts. A separate retained view keeps
unused sources, receipts and other saved evidence searchable. Changing active use
does not move or delete original bytes. Saved bytes alone do not prove usable
content or complete source coverage.

File details show safe origin locations, confirmed capture dates, receipt references
and checksums when recorded. A provisional attribution remains provisional.
Advanced details give portable archive-relative receipt locations or the existing
`verify.py restore` command; the public site does not serve private receipt bodies.

A reviewed dataset-association correction adds an ordinary immutable receipt.
Source-scope policy version `2` can exclude a hash from one `collection_id`,
so an incorrect historical association stays searchable with `--scope all`
while the verified association remains active. An exclusion without a
`collection_id` still applies to every use of that hash. Each correction records
its reason and evidence; it does not rewrite the original receipt or source bytes.

## Source validation and completion

Archive validation establishes which original bytes are held, their supported or
provisional origins, the checks performed on them, and gaps against an explicitly
scoped source inventory. Historical processing receipts and published-table
lineage are separate concerns. Generate processing receipts when creating new
tables; they are not prerequisites for accepting source holdings.

Reviewed source inventories use the existing registry `observations` and
`coverage_measurement` fields. Each expected count retains its actual identities,
scope, source version, local matches and evidence references. A complete edition
list does not establish every underlying record; captured API page coverage does
not establish an atomic or current publisher snapshot. Counts with different
units or selections cannot form a completeness percentage.

Per-table source reviews are observations with `metadata_kind: source-coverage`,
`table`, and `table_version`. The public catalog displays their note only beside
the exact SHA-256 table version reviewed. It keeps those notes out of the generic
observations list, whose public shape omits the version. Required source coverage
stays unknown until the complete required identity set is established. External
sources remain explicit. Historical build evidence retains its separate meaning.

Resolve uncertain origins from retained receipts, native identifiers and actual
file contents, then acquisition scripts or publisher documentation. Record the
supporting evidence and review date in an immutable receipt. Preserve unknown
acquisition times and distinguish a supported source attribution from proof of a
particular historical download. A current publisher page alone does not establish
the version of an older retained file.

Compare holdings with a named publisher inventory, period, version and request
scope. Report held, confirmed missing and unresolved items separately. An index
does not establish possession of its children; alternate formats are not
automatically additional required datasets. State coverage as unknown when the
expected population is unknown. Keep failures and fragments available as evidence
without counting them as complete source files.

Byte checks record their date, input inventory and scope. A prior full audit plus
fresh checks of additions must not be described as a fresh full-archive audit.
Publisher checksum matches provide additional evidence when the checksum names
the exact source representation; they do not establish complete dataset coverage.

## Write and read originals

`CaptureEvidence` uses `spicy_docs.storage.raw_archive.CentralRawArchiveStore`
for original network responses and explicitly marked local source replays.
The default root is `~/Work/corpora/spicyregs/raw`; `SPICYGOV_RAW_ROOT` or the
capture's `archive_root` argument selects another root. Original responses are
retained before parsing. Converted tables and derived inputs stay in the run's
output directory. FEC bulk files, CourtListener exports, PDF attachments, Federal
Register pages and Unified Agenda originals also use this writer. Local `retain_file` and `retain_bytes` calls require
`raw_source=True` only when the input is an original source.

The archive contains:

- `objects/sha256/`: original bytes addressed by their SHA-256 digest. Existing
  `.gz` files are lossless storage wrappers, separate from a publisher's own
  compressed download.
- The current frozen `snapshot.json` and gzip indexes: the consolidated source
  locations and original or provisional receipts. Scope separation writes new
  current indexes and retains the exact prior index and journal bytes as
  receipt-only historical evidence.
- `acquisitions/sha256/`: small immutable receipts for new captures or explicit
  local reviews. Each binds
  raw file identities, source metadata and storage descriptors after the bytes
  are durable. Failed attempts retain receipts without claiming successful data.

New captures reuse verified matching bytes. Receipts commit independently, so a
capture does not rewrite the large base indexes. An interrupted write can leave
an unclaimed object; verification reports it and preserves it for recovery.
The archive's content is sufficient to rebuild holdings without old worktrees.
Use `CentralRawArchiveStore.open(sha256)` to read original bytes, including
losslessly wrapped files. Processing artifact readers use the same store.

The portable, standard-library-only `raw/verify.py` checks both current base
indexes and later acquisition journals. Run `python3 raw/verify.py verify --metadata-only` from the archive's parent directory to check index pins,
references, local paths and stored sizes. Omit `--metadata-only` to rehash all
stored and original bytes. These are byte and inventory checks; they do not
establish complete publisher coverage.

## Rebuild and check before acquisition

Query the selected catalog before acquiring data; a reuse check does not require
rebuilding it. Set these paths for your workspace and run from this repository
with its configured `source-readers` environment:

```sh
STACK="$HOME/Work/spicy-stack"
RAW="$HOME/Work/corpora/spicyregs/raw"
CACHE="$STACK/spicygov/.cache/source-catalog-central"
PUBLIC="$STACK/spicygov/public/source-catalog"
SELECTED_INDEX="$CACHE/index.sqlite"
cd "$STACK/spicy-regs"
uv run --frozen python -m spicy_regs.source_catalog holdings \
  --index "$SELECTED_INDEX"
uv run --frozen python -m spicy_regs.source_catalog query \
  --index "$SELECTED_INDEX" \
  --collection family:bill-family --period 119 --limit 100
```

### Refresh additions without replaying the base inventory

From the workspace root, recreate the dated discovery input offline with
`uv run --project spicy-docs --frozen python scripts/prepare-source-catalog-discovery.py`.
It verifies the retained research captures against their decoded hashes and
reuses the maintained SpicyDocs readers. The path above selects that exact
observation set; repeat `--discovery` for additional retained snapshots. Omitting
the flag builds only the corpus and reviewed registry. It does not discover
publisher catalogs automatically. A refresh requires an explicit acquisition
command described in the workspace guide at
`spicy-docs/docs/sources/source-catalog.md`.

For an unchanged frozen archive snapshot, refresh the selected index with committed
receipt additions and explicitly named content readers:

```sh
uv run --frozen python -m spicy_regs.source_catalog refresh-metadata \
  --index "$SELECTED_INDEX" --output "$RUN/index.sqlite" \
  --expected-generation "$EXPECTED_GENERATION" \
  --publication-reconciliation "$PUBLICATION_SELECTION" \
  --include-acquisitions --refresh-content family:federal-register
```

Use a new `RUN` directory and the selected publication input as described below.
The refresh clones the selected index, imports missing receipt occurrences and
reinterprets the named collections only when their reader, mapper or registry
identity changes. Existing receipt bytes and references are checked through the
archive reader. The base file lists are not replayed. Unrequested interpretations
remain inherited from the recorded base generation; this is not a fresh audit of
every source's bytes. The report records that distinction and the changed scope.

Receipt removals and a changed frozen snapshot require explicit reconciliation or
a full rebuild. A newly arriving receipt is accepted only if the reader included
it and the final indexed receipt set matches the archive. Unread additions stop
selection. Continue through validation, public export and guarded selection below.

### Prepare a rebuild without changing the selection

A holdings-only `build` imports raw inventory and registry scope. It does not
restore selected published tables or their version-bound source notes. To replace
a catalog that includes publication accounting, supply the same pinned publication
selection with `--publication-reconciliation`. Find that input in the selected
index's `publication_reconciliation` metadata under `phaseInputs`; verify its
recorded hash and byte count. It has format `spicy-publication-reconciliation`.
The cache's `selection.json` instead pins public export members and is not that
input. Preserve any selected discovery or publication-observation inputs as well.

Choose a new working directory under the destination cache, outside the raw
archive and folders managed by file-sync software. The candidate and staged
public files must share their respective destination filesystems. A full rebuild can be substantial;
use the publication-only workflow below when raw holdings need no rebuild.

The `matrix` command includes each route's `relationships` and
`selection_policies`. A comparison applies only to its stated identifiers,
period, capture and fields. Matching IDs can coexist with differing fields;
documented publisher reach is separate from observed local holdings. A route
without comparison evidence still reports its relationship as unresolved.
The dated relationship qualification report at workspace path
`docs/source-route-qualification-2026-10-10.md` links the retained comparisons
and destination decisions.

```sh
RUN="$CACHE/rebuild-$(date +%Y%m%d-%H%M%S)"
PUBLICATION_SELECTION=/absolute/path/to/selected-publication-selection.json
test -f "$PUBLICATION_SELECTION" || exit 1
mkdir "$RUN" || exit 1
CANDIDATE="$RUN/index.sqlite"
STAGED_PUBLIC="$RUN/public"
EXPECTED_GENERATION=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["generation"])' "$CACHE/current.json")
uv run --frozen python -m spicy_regs.source_catalog build \
  --corpus "$RAW" --output "$CANDIDATE" \
  --publication-reconciliation "$PUBLICATION_SELECTION" > "$RUN/build.json" || exit 1
uv run --frozen python -m spicy_regs.source_catalog validate \
  --index "$CANDIDATE" > "$RUN/validation.json" || exit 1
uv run --frozen python -m spicy_regs.source_catalog export \
  --index "$CANDIDATE" --output "$STAGED_PUBLIC" > "$RUN/export.json" || exit 1
python3 - "$RUN" <<'CHECK' || exit 1
import json, sys
from pathlib import Path
run = Path(sys.argv[1])
build = json.loads((run / "build.json").read_text())
validation = json.loads((run / "validation.json").read_text())
export = json.loads((run / "export.json").read_text())
assert build["status"] == "validated"
assert validation["integrity"] == "ok" and validation["foreignKeys"] == []
assert build["generation"] == validation["generation"] == export["generation"]
CHECK
```

Run each step only after its predecessor succeeds. `validate` prints findings;
its exit status alone does not establish a clean index. The builder also validates
SQLite and writes `index.sqlite.reconciliation.json` beside the candidate.
Review the candidate's selected tables, exact versions, source notes and coverage
against the intended selection before switching. Successful export validates the
public field restrictions; failed exports can leave incomplete staging files.
Keep a deliberately holdings-only build in a separate cache. Do not replace a
publication-aware selection with it.

### Select the prepared local generation

The maintained `select` command checks the validated report against SQLite
metadata, hashes the candidate once, checks each public member and records its
hash. It rejects a changed current generation, changed candidate or staged files,
active SQLite sidecars, linked paths and public files outside the new generation.
By default it preserves the selected raw snapshot and acquisition-receipt path
set. After reviewing a candidate rebuilt from changed raw inventory, pass
`--allow-inventory-change` to accept that candidate. The previous `current.json`
and `selection.json` must still agree on their raw inventory pins and selected
generation. The candidate must pin the actual raw snapshot digest and exact
acquisition-receipt path set; the flag does not accept a stale candidate. Any
further inventory change during validation still stops the switch. The result
and backup recovery document retain both previous and new inventory pins.

Pause local catalog readers and writers for the switch; individual file renames
are atomic, but the group is not a filesystem transaction. `select` serializes
other `select` calls and writes `current.json` last. It updates the database, both
reconciliation reports, `selection.json`, the public generation and its manifest.
It restores the previous files on a caught failure and retains a
`selection-backup-*` directory whose `recovery.json` maps saved files and moves.
After a process interruption, keep readers and writers paused. Reverse completed
moves **before** restoring the saved targets: restoring only the old index loses
the moved candidate, and leaves its export under the public root. Use the exact
`selection-backup-*` directory from this interrupted local run as `$BACKUP` below.
The journal must be the trusted, unchanged file written by this selector; this is
not an importer for recovery documents from another machine or an unknown source.
The existing selection lock excludes other selectors, but does not pause readers
or other writers. This procedure rolls back to the previous generation, including
an interruption after the last pointer moved; it does not finish the new selection.

<!-- selector-recovery:start -->
```sh
uv run --frozen python - "$BACKUP" <<'PY'
import json
import os
from pathlib import Path
import shutil
import sys

from spicy_regs.source_catalog.selection import plain, require, selection_lock

backup = plain(Path(sys.argv[1]))
recovery = json.loads((backup / "recovery.json").read_text())
saved = [(plain(Path(item["target"])), plain(Path(item["backup"])))
         for item in recovery["saved"]]
moves = [(plain(Path(item["source"])), plain(Path(item["destination"])))
         for item in recovery["moves"]]
cache = saved[0][0].parent
with selection_lock(cache):
    require(all(previous.is_file() for _, previous in saved), "Missing saved selection")
    require(all(source.exists() or target.exists() for source, target in moves), "Missing move endpoints")
    for source, target in reversed(moves):
        if not source.exists():
            os.replace(target, source)
    for target, previous in saved:
        temporary = target.parent / (".restore-" + backup.name + "-" + previous.name)
        if target == cache / "index.sqlite":
            os.link(previous, temporary)
        else:
            shutil.copy2(previous, temporary)
        os.replace(temporary, target)
PY
```
<!-- selector-recovery:end -->

The restored candidate and staging bytes remain available for a retry. Moving
files can change their custody identity. An earlier exporter completion or frozen
receipt qualification must still refuse if that identity changed; do not edit its
pins or bypass the refusal. If the run used `--completion-cache`, use the ordinary
`export` command on the recovered candidate into a **new, absent staging path**
with the same cache, and retry `select` with that path. If it supplied a receipt
snapshot, issue a fresh `freeze-receipts` qualification of the restored selected
index using its recorded digest, and use the new snapshot and digest. This can
repeat export validation and receipt qualification, but does not rebuild the
candidate or repeat the catalog import. Keep the old completion and snapshot as
evidence rather than reassigning their identity.

Keep the backup until the selected generation has been checked. After acceptance,
remove only the backup directory named in this run's `selection-result.json`;
retain the result and verification evidence. Do not remove other selections'
backups. Caught exceptions use the selector's own rollback; the procedure above
is for process interruption and is not a multi-file or concurrent-reader transaction.

```sh
uv run --frozen python -m spicy_regs.source_catalog select \
  --candidate "$CANDIDATE" \
  --reconciliation "$RUN/index.sqlite.reconciliation.json" \
  --staged-public "$STAGED_PUBLIC" --cache "$CACHE" --public "$PUBLIC" \
  --raw-root "$RAW" --expected-generation "$EXPECTED_GENERATION" \
  > "$RUN/selection-result.json"
```

For a reviewed raw inventory change, add `--allow-inventory-change` to the same
command after rebuilding, validating and exporting the candidate. Keep acquisition
separate: selection never writes raw files, and rollback restores the catalog
controls without reverting acquired originals or receipts. Public manifest, detail
and file-page versions must match; versions `1` and `2` are supported.

This is a local catalog selection. It does not modify the registry or raw archive,
refresh publisher inventories, qualify historical processing, or deploy the site.
The selector checks the completed builder's evidence rather than repeating a full
SQLite integrity scan or raw byte audit.

`query` also accepts `--name`, `--sha256`, `--native-id` and `--source-version` for
reuse checks. A matching dataset alone does not prove that the required native
version is held. Combined identity/version filters require evidence from the same
receipt identity or native record; fields from different records cannot combine
into a match. `holdings` reports distinct raw objects, original locations and
native/requested periods. Receipts are queryable but do not inflate raw holdings.
Unknown identities, periods and conflicts remain explicit. The default `active`
scope excludes unused collections and failed, incomplete or explicitly excluded
source files. Use `--scope retained` to find saved evidence and `--scope all` to
check complete custody, including refusal bodies:

```sh
uv run --frozen python -m spicy_regs.source_catalog query \
  --index ../spicygov/.cache/source-catalog-central/index.sqlite \
  --scope retained --name thesaurus --limit 100
uv run --frozen python -m spicy_regs.source_catalog query \
  --index ../spicygov/.cache/source-catalog-central/index.sqlite \
  --scope all --sha256 ORIGINAL_SHA256
```

The registry's `catalog_scope` records the current-use decisions and reasons.
Rebuilding preserves this source-family scope; selected published tables require
the separate publication input described above. The database retains every
inventory row; read-only queries and public exports apply the selected scope. Shared hashes stay
active when they have a usable association with an active collection. New
unreviewed collections remain retained; a publication update that would hide a
published collection fails until its scope is reviewed. Selection establishes
current use of a source family, not consumption of every historical file by a
particular build.

Acquired coverage excludes
HTTP failures, incomplete responses, source fragments, conflicting transport or
native identity evidence, and explicit source-validation refusals. A
later refusal also applies to the same bytes in the same capture run; a separate
accepted capture keeps its own status.

Dataset assignments use explicit receipt fields, reviewed registry bindings,
publisher-native identifiers and named original acquisition directories. The
mapper records the evidence and rule for each assignment. A directory's run date
never becomes a data period.
Requested editions, issue dates, election cycles and individual record dates
remain different kinds of periods. None establishes complete coverage.
Structured period objects and lists retain their separate kind, label and
evidence basis. Supported named fields such as `meeting-date`, `schedule-week`
and `document-date` become searchable dates; descriptive titles remain evidence.

Receipt normalization and content extraction enrich the existing per-file mapping.
Receipt evidence uses relative field locators and one pinned manifest reference;
content evidence points to fields in the checksum-verified original. Acquisition
times, requested periods and observed native periods stay separate. Different
package and record ID formats are preserved; contradictory equivalent package
identities or single-record Congress values remain explicit candidates rather
than qualified identity or period claims.

The content reader recognizes structured lobbying, bill, vote, House communication,
Federal Register, committee meeting and comment records. It examines originals up
to 1 MiB, or 4 MiB for Federal Register pages. These bounds cover the inspected
publisher response formats while limiting individual parser work. Read candidates
retain explicit outcomes for byte limits, unsupported content and extraction errors.
Known unsupported formats are skipped and counted separately. Caption
and playlist filenames never supply meeting dates. Per-record field evidence
survives even when one file contains several native records. Native identity queries
also search those content-derived identities.

A temporary SQLite cache reuses extraction results by original hash and parser
dataset within each rebuild; it is discarded when the connection closes. Original
receipts and acquisition journals are classified as evidence rather than raw source
holdings. The raw archive is read-only during normalization. No additional durable
metadata format is required.

`source_catalog/config/retained-content-reviews.v1.json` records local reviews of
the legacy FEC producer's mixed source and support objects. Findings name exact
decoded hashes and byte sizes, evidence and limits. Both fresh builds and metadata
refreshes pin this file and rehash each reviewed original; larger reviewed objects
are verified in a stream without raising the automatic parser's size limit.
Findings classify content only. They preserve acquisition facts, dataset mappings,
existing refusals and uncertainty about source completeness or acquisition origin.

A local byte-content review can be recorded inside an ordinary immutable receipt
as `receipt.content_review` (`spicygov-content-review`, version `1`). Each decision
names an exact original SHA-256 and byte count, its evidence, classification and
limits. The importer applies these decisions after loading all aliases, including
aliases from earlier captures. It preserves the original receipt fields and adds
a separate reference to the review receipt. A verified error or non-source asset
can change a held alias to `refused-source`; a verified fragment can change it to
`partial-source`. Reviews cannot assert acceptance, replace acquisition facts or
establish complete coverage. Existing failure, conflict and missing-byte states
remain intact. Conflicting reviews fail rather than overwrite earlier decisions.
Dataset associations use separate exact file entries with explicit dataset IDs.

Reconcile a retained acquisition queue against the selected catalog without
rebuilding the index or downloading data:

```sh
uv run --frozen python scripts/reconcile_acquisition_queue.py \
  --queue /path/to/acquisition-gaps.json \
  --index ../spicygov/.cache/source-catalog-central/index.sqlite \
  --output /path/to/reconciled-gaps.json
```

The report preserves each historical observation and adds its current disposition,
matching evidence and grouped review action. The adjacent CSV files provide the
observation and action lists. Old observation IDs never become current raw-file
IDs. Only an acceptable source association with verified original hash and byte
count automatically closes a pinned-file gap. URL matches remain review evidence:
discovery indexes do not satisfy their children, and a retained volume does not
establish its advertised granule identities or native edition. Unknown bindings
remain unknown rather than becoming confirmed missing files. The input queue and
catalog generation are pinned, and generated reports stay outside the raw archive.

The builder checks manifest pins, object references and stored sizes. It pins
its code and registry, rechecks inputs and retained-file state, validates SQLite,
then atomically replaces its requested output index. This does not update the
local selection controls or public export; build to a candidate and use `select`.
A failed rebuild leaves the previous index usable, although its requested report
path records the failure. The archive refuses catalog output inside its own directory. No prior
catalog is an input to a holdings rebuild. The publication reconciliation below
reuses an already selected holdings catalog. The older corpus importer remains available for separately
retained historical inventories; it is not used for the central raw archive.

## Refresh metadata over unchanged raw holdings

Use `refresh-metadata` when the raw archive already matches the selected index
and the change concerns configured source descriptions, routes, evidence, source
scope, bounded legacy FEC classification, or publication accounting. It uses the
same importer for configured IDs and the same content classification rules as a
fresh raw import. It preserves acquired facts and imported collection metadata,
retains superseded configured values as history evidence, and measures custody
and active holdings directly from candidate rows.

The command clones the base index into a new candidate. Large catalogs require a
filesystem supporting APFS `clonefile`; it refuses a large full-copy fallback.
It checks the expected base generation, unchanged raw snapshot and receipt set,
configured references, small changed SQLite tables, and pinned phase inputs.
Its report states that integrity of untouched large tables comes from the base;
it does not repeat a full raw audit or inherited index scan. Concurrent changes
to the source, raw inventory or phase inputs stop preparation. Existing candidate
or report outputs are refused.

```sh
uv run --frozen python -m spicy_regs.source_catalog refresh-metadata \
  --index "$CACHE/index.sqlite" --output "$CANDIDATE" \
  --publication-reconciliation "$PUBLICATION_SELECTION" \
  --expected-generation "$EXPECTED_GENERATION" > "$RUN/refresh.json"
```

`--config` defaults to `source_catalog/config/registry.v1.json`. A
publication-aware base requires `--publication-reconciliation` so refreshed
classifications and descriptions cannot leave stale publication accounting.
A holdings-only base may omit it. Keep the selection document and every exact
pinned publication input in durable evidence storage outside the raw archive,
for example `spicyregs-catalog/evidence/publication/<bundle-sha256>/selection.json`;
references within the bundle may use relative paths. Temporary task folders must
not become selected publication inputs.

The command writes the candidate's matching `.reconciliation.json` report beside
it and prints the same report. Inspect its configured changes, classification
limits, `raw_archive` custody summary and `active_holdings` summary. Export the
candidate to staging, review it, then use `select` as above. A changed raw snapshot
or receipt set requires a holdings rebuild before this metadata-only operation.
Selection remains a separate guarded step.

## Optional publication accounting

This separate workflow describes retained published tables. Its unresolved
processing history does not block source archival, source validation or inventory
cataloging.

Use `reconcile-publication` to add publication accounting to an existing holdings
catalog without reparsing or reclassifying raw files. It pins the selected base
generation, creates a separate candidate, changes publication tables and lineage,
then atomically replaces the requested output. The base catalog's raw input and
producer pins remain unchanged; new evidence and code pins belong to the
reconciliation phase. On macOS, large catalogs require the filesystem's
`clonefile` copy-on-write operation. The command refuses a large ordinary copy or
an uncheckpointed WAL database.

```sh
uv run --frozen python -m spicy_regs.source_catalog reconcile-publication \
  --index "$SELECTED_INDEX" --output "$CANDIDATE" \
  --selection "$PUBLICATION_SELECTION" \
  --expected-generation "$EXPECTED_GENERATION" \
  > "$RUN/index.sqlite.reconciliation.json"
uv run --frozen python -m spicy_regs.source_catalog check-publication \
  --index "$CANDIDATE" --selection "$PUBLICATION_SELECTION"
```

Use the path variables from the rebuild workflow with a fresh `RUN`. Unlike
`build`, `reconcile-publication` returns its report on standard output, so retain
it with the redirection shown. Export the candidate to `STAGED_PUBLIC`, review it,
and use the same `select` command. A nonzero `check-publication` result requires
inspection: `selectionMatches: false` means the supplied selection is not pinned
by this candidate; `missing_provenance` records unresolved historical processing.
Historical processing verification is not a raw-inventory acceptance gate.

The selection has format `spicy-publication-reconciliation`, version `1`, a
`publication` pin and a `receipts` array of pins. Each pin contains `path`,
`sha256` and measured `bytes`. `historical_publications` optionally pins earlier
publication indexes. `rulemaking` optionally contains separate `pointer` and
`manifest` pins, preserving its native snapshot identity. Native publication
indexes use the existing strict publication reader. All selected publication
metadata, normalized processing evidence and their retained native controls live
outside the raw archive.

Normalized receipt documents use `spicy-publication-inputs`, version `1`. Their
`generations` list names the exact `family` and `artifact_digest`, output member
hashes and sizes, observed original-file hashes and sizes, exact parent table
pins, prior generation pins, native evidence input pins, conflicts and an explicit
`complete` flag. The evidence producer verifies native artifact roots, member
manifests and journals before producing this document. The importer pins every
referenced control, follows shared ancestors once and joins each distinct source
hash and size to the existing indexed holdings once. It checks the matched local
file's recorded size and modification time; it does not rehash all raw bytes.
Names and URLs never establish identity.

Current tables receive `missing_provenance` or `conflicting_evidence` with explicit
reasons. The status vocabulary also reserves `verified` and `missing_local_files`
for qualified complete input connections. The supported native evidence currently
records source observations, without proving complete per-table consumption, so
the importer rejects `complete: true` claims. A pinned arbitrary document or a
provisional acquisition receipt cannot establish historic consumption. This
limitation prevents a green parity result until supported native receipt analysis
can prove that connection.

Public `capturedSourceFiles`, `matchedSourceFiles` and `missingSourceFiles` counts
describe known original-file observations at the family and ancestor level.
`sourceFiles` is the same observed set for existing readers. These counts are not
the total inputs required by a table while `inputClosureComplete` is false. Local
paths and file IDs stay internal. Unmapped publication families receive an explicit
publication collection with unknown source association. Historical publication
versions remain separate lineage entries and never replace the current table.
Raw originals unused by the selected publications remain legitimate holdings.

`check-publication` exits nonzero unless every selected current table is verified
and the selection file still matches its exact recorded pin. Add
`--require-verified` to `reconcile-publication` to reject an unresolved candidate
before activation. A fresh holdings build can use the same import with
`build --publication-reconciliation /path/to/publication-selection.json`; the raw
mode guard against arbitrary corpus and table overlays still applies.

## Verify the source archive

For archive verification, including later acquisition receipts:

```sh
uv run --frozen python -m spicy_docs.storage.raw_archive verify \
  --root ~/Work/corpora/spicyregs/raw
# Also rehash every stored and decoded original when a full byte audit is needed:
uv run --frozen python -m spicy_docs.storage.raw_archive verify \
  --root ~/Work/corpora/spicyregs/raw --deep
```

The maintained command above checks the base plus later receipts. Its default
checks pins, references, paths and stored sizes; `--deep` rehashes all bytes.
The portable `verify.py` included in the archive also supports the base and later
receipts, with a different default:

```sh
cd ~/Work/corpora/spicyregs/raw
python3 verify.py verify --metadata-only
python3 verify.py verify
python3 verify.py restore --sha256 DIGEST --destination /new/path/original-file
```

The first command checks metadata; the second rehashes every stored and decoded
original. Restore verifies the recovered original's hash and size. These commands
print their results; they do not update `VERIFIED.json`. Record a successful
result with its exact inventory pins, verifier hash, date and verification scope
before replacing that certificate. Preserve prior byte-audit evidence separately
from newer metadata or addition-only checks.

## Export and mirror

The public exporter keeps the small collection summaries in JSON and writes the
file inventory as separate Parquet files. Each inventory row represents one
recorded source observation. Dataset, publisher, source identity and version,
period, capture time, source URLs, content hash, original size and evidence status
are native columns, so tools can search and group them without opening receipts.
Unknown values remain empty, and conflicting evidence stays explicit. Periods
describe source coverage; capture times describe when the source was obtained.

Public origin URLs omit credentials and query parameters except the registered
CourtListener bulk listing and exact Google Books PDF download locators accepted
by SpicyDocs `disbursements_report_locator`. The Google Books exception requires
the source-reader dependency and retains only the book ID and `output=pdf` query;
other paths, hosts and query keys remain withheld. Export completion pins that
validator's source alongside the catalog exporter. This preserves a recorded
origin link without claiming current download access or report qualification.

Receipt columns point to the separate archived receipt and its entry. The receipt
kind distinguishes a line in a frozen index from an entry in an acquisition
receipt. An original publisher or downloader receipt can have its own reference.
Receipt bodies and private machine paths stay out of the public inventory.
Archive-relative keys remain usable when the archive is mirrored; they do not
claim that a remote copy is already available or publicly readable.

Repeated observations may share a content hash. Count rows to count observations;
count distinct content hashes to count original files. For storage totals,
deduplicate by content hash before summing sizes. A dataset's periods and file
counts do not by themselves prove complete coverage of the publisher's data.

Each public inventory file has a row and byte bound, a checksum, and the same
generation as its collection summary. Readers check those pins and validate the
decoded fields before displaying them. Internal exports retain paths and mapping
evidence in JSON; keep them outside the website's public directory.

```sh
uv run --frozen python -m spicy_regs.source_catalog export \
  --index "$CANDIDATE" --output "$STAGED_PUBLIC"
```

Export to a staging directory and select it with its matching candidate as shown
above; direct export to the selected public directory would leave the local
selection's manifest and member pins stale.

Copy the whole raw archive to mirror it. Transfer original objects before their
acquisition receipts, and transfer the frozen snapshot after its selected base
indexes. Verify the destination before treating the mirror as complete. Catalog
caches and public exports can be rebuilt and need not be mirrored with the data.
The mirror must retain original acquisition receipts and unknown provenance;
copy time must not replace acquisition time.

Expected, advertised, acquired, processed and published coverage remain distinct.
A fresh holdings rebuild establishes local custody; it does not assert a fresh
publisher census, complete time series, usable record count or deployment.
