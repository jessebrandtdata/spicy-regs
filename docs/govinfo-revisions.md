# GovInfo package revisions and replay

Budget volumes, activity reports, committee reports and hearings retain dated
package revisions in their existing tables. `last_modified` is GovInfo's package
addition/update timestamp, which can describe a metadata-only update. It is not
`date_issued`, a hearing's `held_date`, first publication, or proof that every
text change has a distinct timestamp.

The package tables key on `(package_id, last_modified)`. Report parts also key on
`part_id`; sections and CBO letters add their own sequence within that part and
revision. `hearing_bill_links.last_modified` is copied from the hearing package's
summary and joins its exact transcript revision. Its rule digest identifies
processing rules, never a source date.

A successful reread replaces only its package revision, including that revision's
removed children and successful empty link result. Other revisions remain.
Failed and unread scopes retain their prior rows. Missing, malformed and
zone-free dates refuse before a merge can filter a null identity; retained
originals and prior files stay available. A fresh build must resolve that source
evidence explicitly. The dated policy has no automatic migration from prior
package-only policy receipts.

Two conflicting incoming rows with the same complete revision key refuse because
their input order supplies no correction decision. An explicit reread of a
retained key is an incoming correction: it replaces that revision, and a changed
body/text digest is reported. Retained original captures and receipt history keep
the earlier evidence. This does not claim that timestamps uniquely identify all
publisher changes or that this table retains every same-date textual variant.

## Current documents

History tables can contain several revisions of one package. Select the latest
revision before joining. `spicy_regs.govinfo_revisions.latest_package_rows` owns
that selection: compare timestamp instants, then literal date spelling to break
an equivalent-instant tie. It keeps every part of the chosen package revision.
Both print checkpoints and committee-report checkpoint seeding use explicit
latest-revision selection; replaying an older revision does not move a newer
checkpoint backward.

For current report sections, use this same query shape:

```sql
WITH current_reports AS (
  SELECT * FROM committee_reports
  QUALIFY dense_rank() OVER (
    PARTITION BY package_id
    ORDER BY CAST(last_modified AS TIMESTAMPTZ) DESC, last_modified DESC
  ) = 1
)
SELECT r.package_id, r.part_id, r.last_modified, s.seq, s.body
FROM current_reports r
JOIN report_sections s USING (package_id, part_id, last_modified);
```

For current printed citations, select one budget/activity metadata revision
before joining its body. Native subjects name that body `body_version_id`;
processing rows retain its original `text_sha256` spelling.

```sql
WITH current_volumes AS (
  SELECT * FROM budget_volumes
  QUALIFY dense_rank() OVER (
    PARTITION BY package_id
    ORDER BY CAST(last_modified AS TIMESTAMPTZ) DESC, last_modified DESC
  ) = 1
)
SELECT c.* FROM document_citations c
JOIN current_volumes p
  ON c.document_kind = 'budget_volume'
 AND c.document_key = p.package_id
 AND c.body_version_id = p.body_version_id;
```

Metadata-only revisions can share one body digest. Citation occurrence identity
continues to describe that body, so this update does not duplicate occurrences
for each metadata update. The citation tool selects the latest parent before
checking its digest/read state. Generic historical navigation from a citation
returns every matching dated parent and declares that relationship as many.
The citation tool reports the current parent; it does not accept an arbitrary
historical package timestamp as an as-of request. Exact dated report-section
citation keys include `last_modified` between `part_id` and `seq`.

## Incoming corrections versus source dates

`transforms.table_merge.merge_table` intentionally prefers incoming rows for a
repeated complete key. The source date breaks ties within the incoming/prior
population and orders output; it does not override an explicit correction from
a later run. An incoming null is a real replacement value in row-wise merging.
The dated GovInfo callers now distinguish an older-source replay through the
key itself, then choose the latest source revision explicitly for current reads.
The relevant callers are `build_print_citations.build_print_citations` and
`build_committee_reports.build_committee_reports`; both can receive different
retained source versions through injected acquisition and prior readers.

`congress_bills` has a different existing purpose. `build_bill_family` sends
qualified bulk/detail rows through `merge_contract_table`, which enables
column-wise coalescing for bills alone. A non-null incoming field replaces its
prior value; a null means that input does not populate the field and retains the
prior. `update_date` independently keeps the greatest well-formed source update.
Consequently an older incoming title can coexist with the newer retained update
date. That row is a combined current record, not a literal dated source snapshot.
`build_congress_index` also pools list/detail results before calling the shared
merge for its own Congress tables; it does not create bill revision snapshots.
An explicit replay/backfill can supply older source data to these callers. The
shared merge does not claim to infer whether that is a correction or an unwanted
replay. No global date-first ordering change is made for those families.

Offline merge probes demonstrate these rules. They do not establish production
history loss, historical completeness, source timestamp uniqueness or hosted
publication. The retained original summary samples establish literal issue and
modification fields; a paired budget/activity/report/hearing original revision
set was not available in the bounded retained evidence used for this change.
