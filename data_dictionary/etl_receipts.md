A receipt is the paperwork behind one row. Each time our pipeline turns a
publisher's record (an FCC filing, a GAO decision, one rating on a scorecard)
into a row of a table, it writes one receipt to go with it: what it read, which
version of our code read it, how that went, and anything it kept that did not
become a column. The table holds what you query. The receipt holds how the row
got there. ("ETL" in the name is that pipeline: extract, transform, load.)

Receipts are useful for three things:

- **Finding something that is no longer a column.** A table in the newer layout
  keeps the columns people query. A few things people do sometimes want, such
  as the link to a filing's page on the publisher's site, are kept in the
  receipt instead. Where that is so, the table's own notes say it in one
  sentence.
- **Checking where a row came from.** A receipt names what was read to make
  the row.
- **Seeing what did not become a row.** When a pipeline records a record it
  could not convert, it does so as a receipt with no row.

Not every table has receipts yet. Tables are moving to the newer layout one
group at a time, and a table has receipts only after it has been published that
way. `describe_table('etl_receipts')` lists, under `publication`, each group
that has them and the tables in it (`datasets`). A table that is not named
there has no receipts: everything it holds is in the table itself. (The other
`datasets` list in that reply, under `metadata`, names every table that is set
up to have receipts, whether or not it has published any.)

The word has two other uses in this dictionary, and neither is this table.
`fec_receipts` and other campaign-finance wording mean money a committee
received. A note that cites a "receipt" followed by a folder name points at the
maintainers' own evidence files, which are not public.

## The quick way: ask for a value by the row's key

The `read_receipt_fields` tool reads values out of receipts for you. Give it
the table, the keys of the rows you mean and the fields you want. For the link
to the CRS report `R48641` at a particular source update date:

```json
{"table": "crs_reports", "keys": [{"report_id": "R48641", "update_date": "2026-07-17T22:38:55+00:00"}], "fields": ["url"]}
```

`describe_table('crs_reports')` lists, under `receipt_fields`, the table's key
columns with their types and the fields its receipts hold. A key gives every
key column its exact value, so a table with two key columns names both:
`{"committee_id": "C00097238", "cycle": 2012}`. Each row answers `found` or
says why not, and each field says whether the receipt states a value.

The tool reads up to 100 rows in a call. It does not work on a table with more
than 2,000,000 receipts, and says so when asked. For such a table, to join
receipts to a query's rows, or to read a receipt that belongs to no row, use
the queries below.

## How to find the receipt for a row

Without the tool this takes a query. In a table that has receipts, every row
has exactly one receipt whose `outcome` is `accepted`. You find it by the
table's name and the row's key.

1. Ask for the table's key columns. `describe_table('crs_reports')` lists them
   as `identity_columns`; for `crs_reports` those are `report_id` and `update_date`.
2. Write the row's key the way a receipt stores it in `identity_json`: each key
   column as a name and a value, in the order `identity_columns` gives.
3. Select the receipt with that table name and that key.

For the CRS report `R48641` updated at `2026-07-17T22:38:55+00:00`:

```sql
SELECT *
FROM etl_receipts
WHERE dataset = 'crs_reports'
  AND outcome = 'accepted'
  AND identity_json = '["list",[["list",[["str","report_id"],["str","R48641"]]],["list",[["str","update_date"],["datetime","2026-07-17T22:38:55+00:00"]]]]]'
```

A table with two key columns names both. `fec_committee_history` is keyed by
`committee_id` and `cycle`, and its key for committee `C00097238` in 2012 is:

```text
["list",[["list",[["str","committee_id"],["str","C00097238"]]],["list",[["str","cycle"],["int",2012]]]]]
```

Text is written `["str","…"]`, a whole number `["int",2012]` with no quotes
around the number, and a key column that is empty for the row `["null",null]`.
A timestamp is written `["datetime","2026-07-17T22:38:55+00:00"]`.
Spelling and capital letters must match the table's value exactly.

To fetch the receipts of several rows, join on the same expression and build
the key from the table's own column:

```sql
SELECT t.report_id, t.update_date, t.title, r.witnesses
FROM crs_reports t
JOIN etl_receipts r
  ON r.dataset = 'crs_reports'
 AND r.outcome = 'accepted'
 AND r.identity_json = '["list",[["list",[["str","report_id"],["str","' || t.report_id
     || '"]]],["list",[["str","update_date"],["datetime","'
     || replace(strftime(t.update_date AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%f')
                || '+00:00', '.000000+', '+') || '"]]]]]'
WHERE t.report_id IN ('R48641', 'IN12713')
```

Three things to know before relying on this:

- The query reads through all of that table's receipts, so it is slower on the
  largest tables.
- A shorter form you may see, `contains(identity_json, '"R48641"')`, works for
  a table with one key column. Do not use it on a table with several key
  columns: matching one part of the key returns other rows' receipts.
- A key value that contains a double quote or a backslash is stored with a
  backslash in front of each, so the plain joining of text above will not match
  it.

## How to read a value out of a receipt

The three columns ending in `_json` hold text in a form of JSON where every
value is written as a pair: its type, then the value. The text `R48641` is
stored as `["str","R48641"]`, the number 5 as `["int",5]`, a missing value as
`["null",null]`, a record as `["dict",[[name, pair], …]]` and a list as
`["list",[pair, …]]`. This keeps the number 5 apart from the text "5", and a
missing value apart from an empty text. The cost is that the usual JSON
functions cannot fetch a field by its name, so today the working way to read
one is a text pattern:

```sql
SELECT regexp_extract(processing_json, '\["url",\["str","([^"]*)"', 1) AS url
FROM etl_receipts
WHERE dataset = 'crs_reports'
  AND outcome = 'accepted'
  AND identity_json = '["list",[["list",[["str","report_id"],["str","R48641"]]],["list",[["str","update_date"],["datetime","2026-07-17T22:38:55+00:00"]]]]]'
```

Put the field's name where `url` is. The pattern has limits:

- It reads text values only. For a field that is absent, missing or not text it
  returns an empty text, not NULL.
- It returns the first field of that name anywhere in the receipt.
- It stops at the first double quote, so a value that contains one (a title, or
  a list kept as text) comes back cut short. For such a value use the longer
  form, which reads it whole:

```sql
SELECT ('"' || regexp_extract(processing_json, '\["title",\["str","((?:[^"\\]|\\.)*)"', 1) || '"')::JSON ->> '$' AS title
FROM etl_receipts
WHERE dataset = 'crs_reports'
  AND outcome = 'accepted'
  AND identity_json = '["list",[["list",[["str","report_id"],["str","R48641"]]],["list",[["str","update_date"],["datetime","2026-07-17T22:38:55+00:00"]]]]]'
```

To see which fields a table's receipts hold, look at one:
`SELECT processing_json FROM etl_receipts WHERE dataset = 'crs_reports' AND outcome = 'accepted' LIMIT 1`.

## What a receipt contains

One row per receipt. The first columns say which row the receipt belongs to;
the rest say what was read and what happened. On the server, `etl_receipts` is
one table holding the receipts of every table that has them. In the published
files, each group of tables has its own `etl_receipts.parquet` beside its
tables, and the publication index names it.

<!-- columns -->

## What a receipt does not tell you

- **Whether the publisher's record is right, current or approved.** `outcome`
  describes our processing only. `accepted` means the pipeline turned the record
  into a row; it says nothing about what an agency decided. Where a publisher
  states a status of its own, that is a column of the table.
- **What the row said before.** The server holds one build of each table, the
  one published now, with that build's receipts. A receipt for a row carried
  over from an earlier build notes the earlier receipts in `diagnostic_json`;
  the earlier builds themselves cannot be queried here. Tables that retain source
  revisions, such as `crs_reports`, expose those revisions as separate rows in the
  current build.
- **Where to open the publisher's page.** `witnesses` names what the pipeline
  read, and that is not always a page you can visit. For some tables it is one
  of the pipeline's own working files, named by a path on the machine that ran
  the build. A link to the publisher, where we hold one, is a column of the
  table or a field in `processing_json`.
- **That nothing was missed.** A table whose receipts are all `accepted` shows
  that no failure was recorded, not that every record the publisher holds was
  read. What a table covers is stated in its own coverage note.
- **Anything about a table that has no receipts yet.** That table has not moved
  to the newer layout. Nothing is missing from it.
