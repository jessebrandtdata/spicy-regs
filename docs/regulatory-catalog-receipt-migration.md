# Regulatory catalog subject and receipt writes

Regulatory catalog writes use native business fields in the configured namespace
with `_native` appended. The prior namespace is retained but never read.
`regulatory_catalog.ensure_native` prepares empty physical tables, then commits a
checked initialization receipt; reads require that receipt, so a crash after
preparation cannot select a half-initialized dataset. It does not convert the
prior namespace's rows: that conversion was removed on 2026-10-03 with the other
legacy paths. The first writer to reach an uninitialized catalog therefore starts
an empty dataset. Populating it from retained rows is an explicit operator step,
done before any scheduled writer runs; its order, checks and rollbacks are in
[the conversion runbook](native-family-conversion.md#the-regulations-unit-pending-the-bulk-writer).

`replace_native` reads the prior subject and receipt in one transaction, compares
any caller-provided prior and snapshot, converts the replacement, and writes the
subject and its current receipt in the same multi-table Iceberg transaction.
The readback checks exact identity, policy, content digest, and restored values.
Prior source witnesses carry forward. Exact source records remain in receipts;
their witness digests refer to their canonical `exact_json` bytes.

Processing and retry fields are reconstructed only by `processing_table` from a
qualified pair. The scheduler, repair, text enrichment, field fill, seed, and
backfill callers use that boundary. Refused writes roll back their subjects and retain a refused attempt receipt.
Unselected older or duplicate source observations remain rejected receipts. Prior table and
receipt versions remain subject to the catalog's snapshot retention policy.

`export_pair` captures native subjects and receipts together and qualifies them.
The comments mirror no longer uses it: it copies the comments table alone and
publishes no receipts ([comments: leaving per-row receipts](comments-receipt-archive.md)).
It pins the comments table's identity and schema, records the receipt table's
identity beside it, and allows only compaction commits to move the comments
table's pin; a receipt-only append publishes nothing. Fixed public mirrors
contain native subject fields.
Receipt-only checkpoints use selected generations before acquisition state
advances. No registry override enables a legacy production writer.

Local tests exercise rollback, stale priors, invalid receipts, migration, scope,
and the scheduler and repair entrypoints. The disposable Apache Iceberg REST
fixture additionally exercises actual multi-table commits, interrupted migration,
crash recovery, concurrent conflicts, and forward undo. The configured catalog
must support multi-table transactions; unsupported catalogs raise rather than
falling back to separate subject and receipt commits.
