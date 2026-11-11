# Independent U.S. Code qualification

The builder produces native edition, structure and section tables. Two independent auditors check those outputs against every member of the selected original archive: `uscode_editions_audit` for one annual XHTML archive, `uscode_xml_audit` for one release-point XML archive (a whole corpus or one title). A successful build and a successful qualification are separate results. The tables publish no per-row receipts, so each auditor checks every row against the original itself.

## Inputs and scope

Supply a candidate directory and the exact selection manifest used to build it. The annual auditor also takes an explicit expected year; its supported scope is one selected annual archive from 1994 through 2024, and the manifest year must match. The XML auditor takes one `release-point-corpus` or `release-point-title` source at any release point. Scope support does not establish that an original archive or installed runtime has passed complete qualification. Multiple archives require separate audits.

The annual auditor derives expectations from original ZIP bytes, native comments, its own field stack and standard HTML parser events. The XML auditor derives them with ElementTree from literal XML: every member's native title, release point, document type and identifier (held to the title its member name selects), and every section and chapter in postorder with its fields, body text, null versus empty bodies, context and unclassified children. Maintained components supply output schemas only. Neither auditor calls the producer, acquisition readers or section readers to derive source expectations.

Both check original member order and identities, literal title metadata, duplicate native positions, hierarchy, rendered text and every row's identity and position, and check public fields, including recursively decoded JSON, for private locators and credential fields. The annual auditor also checks carried publication years, supporting files, document pairing, named field spans and nesting, and decoding issues.

## Commands and results

Use the same pinned interpreter and package tree for production and audit:

```sh
<pinned-python> -I -B -m spicy_regs.sources.uscode_editions \
  --manifest <selection.json> --output <fresh-candidate-directory>

<pinned-python> -I -B -m spicy_regs.sources.uscode_editions_audit \
  <candidate-directory> --selection <selection.json> --annual-year 1995 \
  --output <fresh-qualification.json>

<pinned-python> -I -B -m spicy_regs.sources.uscode_xml_audit \
  <candidate-directory> --selection <selection.json> --output <fresh-qualification.json>
```

The interpreter is the launcher; the Code command has no `--installed-python` flag. Console entry `audit-uscode-editions` is the annual auditor's packaging integration. The module commands work without that registration.

The library calls `audit(candidate, selection, annual_year=1995)` and `audit(candidate, selection)` return a report and write no files. Each command exclusively reserves a fresh report outside the candidate before auditing. It refuses existing paths and aliases to inputs, then writes one success or failure result. Exit zero requires the complete audit to pass. A failure returns nonzero and retains a failure report when the fresh path was reserved. An interrupted or incomplete report never establishes qualification.

The report binds source, selection, generation and executing auditor hashes, checked row and member counts, scope and limitations, and `evidence_sha256`: the digest of every file it read, which the additive preparation (`uscode_edition_publication`) requires. Retain the report alongside exact command, package/runtime hashes, resource logs and independent result review. The auditor verifies its inputs, outputs and package tree remain unchanged during its checks; it does not supervise operating-system resources or prove a complete runtime closure itself.

## Immutable history and resource limits

Any change to the complete reader package tree changes future generation identities. Existing candidates and qualification reports remain bound to their original reader trees; an auditor refuses a candidate from a different tree. Build a fresh candidate under the same pinned tree before qualifying it. Rewriting a selection's source locator also changes its raw bytes and therefore its generation.

The annual auditor reads one member and Arrow batches of 32 rows at a time, with a 128 MiB member bound. The XML auditor holds one member's whole ElementTree and its observations at a time, within the builder's 192 MiB archive and 128 MiB member bounds; the largest member, Title 42, is about 114 MB of XML. These bounds do not establish an aggregate memory, disk or thread limit: run under external resource supervision.

Complete private audits of selected 1994 and 1995 originals provide lineage for the annual implementation; the 2000 year extension added policy and synthetic fixture coverage. Release point 119-102 was first qualified in 2026-10 by an audit outside this repository. On 2026-11-11 `uscode_xml_audit` qualified 119-138, and a rebuild of 119-102 under SpicyDocs 0.85 both passed it and reproduced every published 119-102 row in every column. None of these qualify a newly installed auditor, other archives, legal interpretation, publisher typography, full history, publication or retirement. Those decisions require their own evidence and authority.
