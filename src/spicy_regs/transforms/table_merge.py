"""Shared merge helper for incremental, deduplicated Parquet tables.

Every rollup that ingests an external source and republishes an incremental
table follows the same three steps: best-effort download the prior published
table from R2, dedup the union of prior + freshly fetched rows on an identity
key (preferring the fresh row), and order the result by a version column. This
is that step, parameterised over the column tuple, identity and version column
so a table with any key shape can use it. A table stored as several files runs
the same step once per partition it touches (:func:`merge_partitioned_table`).
"""

from __future__ import annotations

import os
import shutil
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2

ReplacementScope = tuple[str, Collection[str]] | tuple[tuple[str, ...], Collection[tuple[str, ...]]]

#: The one file a rewritten partition holds; ``generations.build_generation`` takes ``part-NNNNNN.parquet``.
PARTITION_MEMBER = "part-000000.parquet"


def set_column(table: pa.Table, name: str, values: Sequence[str | None]) -> pa.Table:
    """``table`` with ``name`` holding ``values`` as VARCHAR, in its place, or appended where the table lacks it."""
    column, index = pa.array(values, type=pa.string()), table.schema.get_field_index(name)
    return table.set_column(index, name, column) if index >= 0 else table.append_column(name, column)


def write_in_place(table: pa.Table, path: Path) -> None:
    """Replace the merged table at ``path`` with ``table``, its file metadata (checkpoints) kept with its schema."""
    temporary = path.with_suffix(".tmp.parquet")
    pq.write_table(table, temporary, compression="zstd", row_group_size=50_000)
    temporary.replace(path)


def merge_local_prior(
    con,
    *,
    columns: tuple[str, ...],
    identity: str | tuple[str, ...],
    order_by: str,
    prior_file: Path | None,
    new_file: Path,
    out_file: Path,
    row_group_size: int = 50_000,
    kv_metadata: Mapping[str, str] | None = None,
    retire: str | None = None,
    derived: Mapping[str, str] | None = None,
    renamed: Mapping[str, str] | None = None,
) -> None:
    """Merge the fresh ``new_file`` over a local prior on one identity column.

    The shared idiom behind the ingest rollups: union the prior file (when it
    exists — the caller already downloaded it where it downloads one) with the
    fresh rows, keep the fresh row on a repeated identity, refuse a NULL
    identity, and publish ordered by ``order_by`` with small row groups for
    scoped reads. One scan per side, deduplicated once, instead of one
    hand-rolled copy per rollup. A composite ``identity`` names its columns in
    order; only the first must be non-NULL (a SAM registration's EFT indicator
    is usually NULL, and NULLs partition together).

    ``retire`` is a SQL predicate over prior rows, for a caller whose fresh rows
    are the complete population of a slice: a prior row it holds TRUE for is
    dropped before the union, so what the slice no longer contains is retired.
    A row it holds NULL for is kept.

    ``derived`` maps an output column to a SQL expression over ``columns``,
    appended after them and recomputed from every merged row each run, so a
    prior row gains it and it never drifts from the columns it reads.

    A column the prior lacks is selected as NULL, as :func:`merge_table` does,
    so appending a column is a NULL backfill on the published rows, not a
    migration. ``renamed`` maps an output column to the name a published prior
    spells it with: a prior holding that name and not the new one reads it as
    the new column, so a rename carries the prior's own values and never needs
    the source read again.
    """
    keys = (identity,) if isinstance(identity, str) else tuple(identity)
    cols = ", ".join(f'"{c}"' for c in columns)
    new_path = str(new_file).replace("'", "''")
    if prior_file is not None and prior_file.exists():
        prior_path = str(prior_file).replace("'", "''")
        held = set(pq.read_schema(prior_file).names)
        former = {c: old for c, old in (renamed or {}).items() if c not in held and old in held}
        if former:
            logger.info("Prior {} spells {}; reading them renamed", prior_file.name,
                        ", ".join(f"{c} as {old}" for c, old in former.items()))
        if absent := [c for c in columns if c not in held and c not in former]:
            logger.info("Prior {} lacks {}; NULL-filling", prior_file.name, ", ".join(absent))
        prior_cols = ", ".join(
            f'"{c}"' if c in held else f'"{former[c]}" AS "{c}"' if c in former else f'NULL AS "{c}"' for c in columns
        )
        union = (
            f"SELECT {prior_cols}, 0 AS _src FROM read_parquet('{prior_path}') "
            + (f"WHERE ({retire}) IS NOT TRUE " if retire else "")
            + f"UNION ALL BY NAME SELECT {cols}, 1 AS _src FROM read_parquet('{new_path}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_path}')"
    out_path = str(out_file).replace("'", "''")
    extra = "".join(f', ({expression}) AS "{name}"' for name, expression in (derived or {}).items())
    metadata_clause = ""
    if kv_metadata is not None:
        metadata_clause = ", KV_METADATA ?"
    con.execute(
        f"""
        COPY (
            SELECT {cols}{extra} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(f'"{k}"' for k in keys)} ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE "{keys[0]}" IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY {order_by}
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size}{metadata_clause});
        """,
        ([kv_metadata] if kv_metadata is not None else None),
    )


def retired_rows(con, *, identity: tuple[str, ...], prior_file: Path, new_file: Path, retire: str) -> list[list]:
    """The identities of prior rows ``retire`` drops that ``new_file`` does not supply again, in key order.

    What :func:`merge_local_prior` removes for the same ``retire``: a caller journals it as a
    ``rows-retired`` event, which qualification reconciles against the rows actually removed.
    """
    prior, new = (str(path).replace("'", "''") for path in (prior_file, new_file))
    keys = ", ".join(f'"{k}"' for k in identity)
    same = " AND ".join(f'n."{k}" IS NOT DISTINCT FROM p."{k}"' for k in identity)
    rows = con.execute(
        f"SELECT {keys} FROM read_parquet('{prior}') p WHERE ({retire}) "
        f"AND NOT EXISTS (SELECT 1 FROM read_parquet('{new}') n WHERE {same}) ORDER BY {keys}"
    ).fetchall()
    return [list(row) for row in rows]


def prior_scratch_path(output_dir: Path, name: str) -> Path:
    """The local path :func:`merge_table` caches/reuses the prior table under.

    Public so a caller that must inspect the prior table *before* merging —
    e.g. to size an incremental fetch window from its max version column — can
    download to this exact path first; a successful download means
    :func:`merge_table` finds the file present and does not re-download it. A
    failed download (a cold start) writes nothing, so pass the caller's own
    result as ``prior_present`` so a known-absent prior isn't asked for twice.
    """
    return output_dir / f"_{name}_prior.parquet"


def published_table(
    output_dir: Path, name: str, download_prior: Callable[[str, Path], bool] = r2.download
) -> Path | None:
    """The published ``name`` table, downloaded to :func:`prior_scratch_path` unless already there.

    ``None`` when nothing is published yet, which every caller treats as a cold
    start rather than an error. This is the one idiom behind every "read a
    published table before merging": a transform's own prior, for a watermark
    or a held-set, and the merge-time joins against another rollup's published
    output. Pass the result's presence on to :func:`merge_table` as
    ``prior_present`` when the table is the caller's own, so a known-absent
    prior is not asked for twice.
    """
    path = prior_scratch_path(output_dir, name)
    return path if (path.exists() or download_prior(f"{name}.parquet", path)) else None


def download_prior_members(remote_key: str, directory: Path) -> list[Path]:
    """:func:`r2.download_members` for a prior; none while R2 is unconfigured or no family publishes the table.

    Those two are a cold start, as :func:`r2.download` reads them, not the failures ``download_members`` raises for.
    """
    from spicy_regs.sources.publication import current_index, table_owner

    public_url = os.getenv("R2_PUBLIC_URL")
    if not public_url or table_owner(current_index(public_url), remote_key) is None:
        return []
    return r2.download_members(remote_key, directory)


def prior_members_path(output_dir: Path, name: str) -> Path:
    """The directory :func:`published_members` downloads a split table's prior into, and reuses when present."""
    return output_dir / f"_{name}_prior"


def published_members(
    output_dir: Path, name: str, download_members: Callable[[str, Path], Sequence[Path]] = download_prior_members
) -> list[Path] | None:
    """The published ``name`` table's files under :func:`prior_members_path`, downloaded unless already there.

    A split table's members sit at ``<name>/<col>=<value>/part-NNNNNN.parquet``, each checked against its pin; a table
    still published as one file sits at ``<name>.parquet``, which :func:`merge_partitioned_table` splits. ``None``
    means nothing is published, a cold start. The split table's counterpart of :func:`published_table`.

    The download lands in a sibling ``.partial`` directory renamed into place once complete, so a present prior
    directory always holds every member: a failed or killed download leaves none to be read as the prior.
    """
    directory = prior_members_path(output_dir, name)
    if not directory.exists():
        partial = directory.with_name(directory.name + ".partial")
        shutil.rmtree(partial, ignore_errors=True)
        partial.mkdir(parents=True)
        try:
            download_members(f"{name}.parquet", partial)
        except BaseException:
            shutil.rmtree(partial)
            raise
        partial.rename(directory)
    return sorted(directory.rglob("*.parquet")) or None


@dataclass(frozen=True, slots=True)
class Partitioning:
    """A table stored one file per value of ``column``, which ``value`` derives from the identity column ``source``.

    The value must be a function of the identity (multi-file design §4.2): a row then lives in one partition only, so
    carrying an untouched partition forward can never keep a stale copy of a row whose fresh copy lands in another.
    """

    column: str
    source: str
    value: Callable[[str], str]


def merge_partitioned_table(
    output_dir: Path,
    *,
    name: str,
    columns: tuple[str, ...],
    identity: tuple[str, ...],
    version_column: str | None,
    rows: Iterable[Mapping[str, object]],
    partitioning: Partitioning,
    prior: Sequence[Path] | None,
    replace_parents: ReplacementScope | None = None,
) -> Path:
    """Merge ``rows`` into the table stored one file per partition; return its directory, ``output_dir / name``.

    A partition is merged, by :func:`merge_table`, when it has fresh rows, a ``replace_parents`` scope, or a file
    whose columns are not ``columns``: the contract gained a column since it was written, and every member of a
    split table must share one column list. It is written as ``<column>=<value>/part-000000.parquet`` in a directory
    of its own, so no prior part file survives beside it to publish its rows twice. Every other prior partition is
    carried byte for byte, which publication copies server-side (multi-file design §4.3). A scope that does not name
    ``partitioning.source`` could reach any partition, so it rewrites them all.

    ``prior`` is what :func:`published_members` returned. The first split's prior is one ``<name>.parquet``: it is
    cut by ``partitioning.value`` over ``source`` into files without the partition column, so the column rule merges
    every one and fills the column with its partition's value. A fresh row's partition column is set the same way,
    from ``source``, whatever the row stated.

    The prior is only read: an untouched partition is hard-linked into the output, and the prior directory goes once
    the whole table is written, so a run that fails part-way leaves its prior whole for the next attempt.

    Cost: one merge per touched partition, each reading only that partition's prior, and one footer read per prior
    file; the first split adds one pass over the prior. An empty table has no partition, hence no member, which
    ``build_generation`` refuses.
    """
    column, source = partitioning.column, partitioning.source
    if column not in columns or source not in identity:
        raise ValueError(f"{name}: the partition column must be a column, derived from an identity column")
    out, work = output_dir / name, output_dir / f"_{name}_partitions"
    for stale in (out, work):  # an earlier run's output or leftovers here are not this run's prior
        shutil.rmtree(stale, ignore_errors=True)
    out.mkdir(parents=True)
    work.mkdir()
    held, first_split = _held_partitions(prior, name, partitioning, work)
    fresh: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        if (key := row.get(source)) is not None:  # the merge drops a NULL identity as well
            value = partitioning.value(str(key))
            fresh[value].append({**row, column: value})
    scopes, everywhere = _scopes_by_partition(replace_parents, partitioning)
    reshaped = {value for value, paths in held.items() if any(pq.read_schema(path).names != list(columns)
                                                                for path in paths)}
    touched = set(fresh) | set(scopes) | reshaped | (set(held) if everywhere else set())
    rewritten = 0
    for value in sorted(set(held) | touched):
        directory = out / f"{column}={value}"
        if value not in touched:
            directory.mkdir()
            for path in held[value]:
                _link(path, directory / path.name)
            continue
        if value not in held and value not in fresh:
            continue  # a scope over a partition that holds nothing
        # No ``col=value`` in this path: DuckDB would read one as a Hive column beside the stored one.
        scratch = work / value
        scratch.mkdir()
        if value in held:
            _prior_file(held[value], prior_scratch_path(scratch, name))
        merged = merge_table(
            scratch,
            name=name,
            columns=columns,
            identity=identity,
            version_column=version_column,
            rows=fresh.get(value, ()),
            remote_key=PARTITION_MEMBER,
            download_prior=lambda _key, _path: False,
            prior_present=value in held,
            replace_parents=replace_parents if everywhere else scopes.get(value),
            prior_constants={column: value},
        )
        directory.mkdir()
        os.replace(merged, directory / PARTITION_MEMBER)
        rewritten += 1
    shutil.rmtree(work)
    shutil.rmtree(prior_members_path(output_dir, name), ignore_errors=True)
    logger.info("{}: {:,} of {:,} partitions rewritten ({:,} for their columns){}", name, rewritten,
                len(set(held) | set(fresh)), len(reshaped), " (first split)" if first_split else "")
    return out


def _held_partitions(
    prior: Sequence[Path] | None, name: str, partitioning: Partitioning, work: Path
) -> tuple[dict[str, list[Path]], bool]:
    """The prior's files by partition value, and whether it was one unsplit file this run splits."""
    if not prior:
        return {}, False
    if len(prior) == 1 and prior[0].name == f"{name}.parquet":
        return _split_prior(prior[0], partitioning, work / "split"), True
    held: dict[str, list[Path]] = defaultdict(list)
    for path in prior:
        column, separator, value = path.parent.name.partition("=")
        if path.parent.parent.name != name or column != partitioning.column or not separator:
            raise ValueError(f"{name}: a prior member is not <name>/{partitioning.column}=<value>/<part>: {path}")
        held[value].append(path)
    return dict(held), False


def _split_prior(path: Path, partitioning: Partitioning, into: Path) -> dict[str, list[Path]]:
    """Cut one prior file by partition in one pass, into files without the partition column; ``path`` is kept.

    The value comes from ``partitioning.value`` over the distinct ``source`` values, the one derivation the rows use,
    not a SQL restatement of it. A row whose ``source`` is NULL is dropped, as the merge drops a NULL identity. The
    files lack the column (DuckDB writes a ``PARTITION_BY`` column only when asked), which is what has
    :func:`merge_partitioned_table` merge each and fill the column with its partition's value.
    """
    from spicy_regs.sources.publication import parquet_scan

    column, source = partitioning.column, partitioning.source
    scan, target = parquet_scan([str(path)]), str(into).replace("'", "''")
    con = _duckdb_session(into.parent)
    try:
        values = [value for (value,) in con.execute(f'SELECT DISTINCT "{source}" FROM {scan}').fetchall()
                  if value is not None]
        con.register("partition_of", pa.table({
            source: pa.array(values, type=pa.string()),
            column: pa.array([partitioning.value(value) for value in values], type=pa.string()),
        }))
        present = [str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()]
        kept = ", ".join(f'p."{c}"' for c in present if c != column)
        con.execute(
            f'COPY (SELECT {kept}, m."{column}" FROM {scan} p JOIN partition_of m ON p."{source}" = m."{source}")'
            f" TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (\"{column}\"))"
        )
    finally:
        con.close()
    held: dict[str, list[Path]] = defaultdict(list)
    for part in sorted(into.rglob("*.parquet")):
        held[part.parent.name.partition("=")[2]].append(part)
    return dict(held)


def _scopes_by_partition(
    replace_parents: ReplacementScope | None, partitioning: Partitioning
) -> tuple[dict[str, ReplacementScope], bool]:
    """Each partition's share of a replacement scope, or ``True`` when the scope cannot name its partitions."""
    if replace_parents is None:
        return {}, False
    columns, values = replace_parents
    named = (columns,) if isinstance(columns, str) else tuple(columns)
    if partitioning.source not in named:
        return {}, True
    at = named.index(partitioning.source)
    scopes: dict[str, list[tuple[str, ...]]] = defaultdict(list)
    for value in values:
        row = (value,) if isinstance(value, str) else tuple(value)
        scopes[partitioning.value(row[at])].append(row)
    return {partition: (named, rows) for partition, rows in scopes.items()}, False


def _link(path: Path, into: Path) -> None:
    """``into`` names ``path``'s bytes: a hard link, or a copy where the filesystem has none."""
    try:
        os.link(path, into)
    except OSError:
        shutil.copyfile(path, into)


def _prior_file(paths: Sequence[Path], into: Path) -> None:
    """One partition's prior as the one file ``merge_table`` reads: linked, or its parts concatenated by column name.

    By name, so parts written before and after a column was appended still line up; a column one part lacks is NULL.
    """
    if len(paths) == 1:
        _link(paths[0], into)
        return
    listed = ", ".join("'" + str(path).replace("'", "''") + "'" for path in paths)
    target = str(into).replace("'", "''")
    con = _duckdb_session(into.parent)
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet([{listed}], hive_partitioning = false, union_by_name = true)) "
                    f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()


def merge_table(
    output_dir: Path,
    *,
    name: str,
    columns: tuple[str, ...],
    identity: tuple[str, ...],
    version_column: str | None,
    rows: Iterable[Mapping[str, object]],
    remote_key: str,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
    coalesce_prior: bool = False,
    replace_parents: ReplacementScope | None = None,
    backfill_prior: Mapping[str, str] | None = None,
    parquet_metadata: Mapping[str, str] | None = None,
    prior_constants: Mapping[str, str] | None = None,
) -> Path:
    """Merge freshly fetched ``rows`` against the prior ``remote_key`` table.

    Writes ``output_dir / remote_key``: the union of the prior table
    (best-effort downloaded via ``download_prior``, defaulting to
    :func:`spicy_regs.sources.r2.download`) and ``rows``, deduplicated on
    ``identity`` (every identity column must be non-null; the fresh row wins
    over the prior one on a repeated identity), ordered by ``version_column``
    descending then ``identity`` — or by ``identity`` alone when
    ``version_column`` is ``None``, which is how the two contracts with no
    freshness axis (``section_diff_items``, ``financial_changes``, both ordered
    by a sequence number within their parent) publish.
    ``columns`` becomes an all-VARCHAR Arrow schema, so callers coerce before
    calling. An absent prior table (first run, or ``download_prior`` returning
    ``False``) degrades to publishing ``rows`` alone, which is a full backfill,
    not an error.

    A prior table missing some of ``columns`` is also not an error: each absent
    column is selected as a VARCHAR NULL, so appending a column to a contract
    is a NULL backfill on the rows already published rather than a migration.
    ``congress_bills`` is the live case — its first ten columns are frozen
    because other repositories pin that prefix by digest, and the bill family
    appends the rest.

    ``coalesce_prior``: merge column-wise instead of row-wise. The default
    (``False``) is row replacement — a fresh row wins whole, and a NULL in it
    is a real value that overwrites. Set it when a table holds **rows from two
    sources that state different column subsets**, where a fresh NULL means "I
    do not populate this column" rather than "this is now empty": the merge
    becomes a FULL OUTER JOIN on the identity emitting ``COALESCE(fresh,
    prior)`` per column, except where :data:`COALESCE_RULES` names a column's
    own rule. Without it, a narrower row does not merely NULL the other
    source's columns, it *drops* them, and the loss is small enough that the R2
    shrink guard (0.5) does not notice. :func:`merge_contract_table` sets the
    flag for ``congress_bills`` alone.

    ``prior_present``: pass ``False`` when the caller already tried
    :func:`prior_scratch_path` and knows the prior is absent, skipping a second
    ``download_prior`` call for the same known-absent object; ``True`` or the
    default ``None`` leave the disk-then-download behavior unchanged.

    ``replace_parents`` names a parent column and the successfully evaluated
    parent IDs: remove their prior relationship rows before merging, including
    when their fresh result is empty, while unread or failed parents keep their
    rows. A tuple of column names and a collection of matching value tuples
    replaces composite scopes, such as one file within one package, without
    modifying the retained prior file before the merge succeeds.

    ``backfill_prior`` maps a column to another column of the same table whose
    value a prior row takes where its own is NULL or absent. It exists for a
    column appended to the identity: the NULL-fill above would leave that part
    of every prior row's identity NULL, and the identity test would then drop
    every prior row without a word. The committee-report tables are the case:
    ``{"part_id": "package_id"}`` spells a row published before
    ``(package_id, part_id)`` as the package's one part (decision 29). It is
    applied in the prior's select, before the identity test and alongside
    ``replace_parents``.

    ``parquet_metadata`` writes processing checkpoints in the same artifact as
    the rows, including a successful read producing zero rows; callers supply
    the complete metadata to retain, it is not inferred from row presence.

    ``prior_constants`` gives a column the prior lacks one value on every prior
    row instead of NULL: a split table's partition column, which every row of
    one partition holds by construction (:func:`merge_partitioned_table`).
    """
    out_file = output_dir / remote_key
    prior_file = prior_scratch_path(output_dir, name)
    new_file = output_dir / f"_{name}_new.parquet"

    schema = pa.schema([(c, pa.string()) for c in columns])

    # 1. Pull the prior table (best effort — absence just means full backfill).
    # ``prior_present is False`` is the caller telling us it already tried and
    # found nothing; anything else falls back to disk-then-download.
    if prior_present is False:
        have_prior = False
    else:
        have_prior = prior_file.exists() or download_prior(remote_key, prior_file)
    if have_prior:
        logger.info("{}: merging against prior table {}", name, prior_file)
    else:
        logger.info("{}: no prior table found — full backfill", name)

    # 2. Shape the freshly fetched rows into a "new rows" parquet.
    row_list = list(rows)
    from spicy_regs.govinfo_revisions import DATED_TABLES, revision_instant

    if name in DATED_TABLES and "last_modified" in identity:
        # Refuse before the generic NULL-identity filter can erase an undated
        # input or prior. The retained source and selected prior stay untouched.
        for row in row_list:
            revision_instant(row.get("last_modified"))
        if have_prior:
            if "last_modified" not in pq.read_schema(prior_file).names:
                raise ValueError(f"{name}: prior lacks publisher last_modified; rebuild from retained originals")
    table = pa.Table.from_pylist(row_list, schema=schema) if row_list else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("{}: {:,} fresh rows this run", name, len(row_list))

    # 3. Merge prior + new, dedup on identity preferring the new row.
    con = _duckdb_session(output_dir)

    cols = ", ".join(columns)
    key_cols = ", ".join(identity)
    order_by = f"{version_column} DESC, {key_cols}" if version_column else key_cols
    not_null = " AND ".join(f"{c} IS NOT NULL" for c in identity)
    # Two rows from the same source sharing an identity are resolved by the
    # version column, not by whichever the scan happened to reach first, so a
    # re-run over the same input produces the same file.
    newest = f"_src DESC, {version_column} DESC" if version_column else "_src DESC"

    if name in DATED_TABLES and "last_modified" in identity:
        if have_prior and con.execute(
            "SELECT EXISTS(SELECT 1 FROM read_parquet(?) WHERE last_modified IS NULL "
            "OR NOT regexp_full_match(last_modified, "
            "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})') "
            "OR try_cast(last_modified AS TIMESTAMPTZ) IS NULL)", [str(prior_file)]
        ).fetchone()[0]:
            con.close()
            raise ValueError(f"{name}: prior has missing or invalid last_modified; rebuild from retained originals")
        # Two conflicting incoming readings at one coordinate have no chosen
        # correction order. Refuse rather than make parquet scan order policy.
        duplicate = con.execute(
            f"SELECT {key_cols} FROM read_parquet(?) GROUP BY {key_cols} "
            f"HAVING count(DISTINCT row({cols})) > 1 LIMIT 1", [str(new_file)]
        ).fetchone()
        if duplicate:
            con.close()
            raise ValueError(f"{name}: conflicting incoming rows at one publisher revision: {duplicate}")
        digest = "sha256" if "sha256" in columns else "text_sha256" if "text_sha256" in columns else None
        if have_prior and digest and set(identity + (digest,)) <= set(pq.read_schema(prior_file).names):
            same = " AND ".join(f"p.{key} = n.{key}" for key in identity)
            conflicts = con.execute(
                f"SELECT count(*) FROM read_parquet(?) p JOIN read_parquet(?) n ON {same} "
                f"WHERE p.{digest} IS DISTINCT FROM n.{digest}", [str(prior_file), str(new_file)]
            ).fetchone()[0]
            if conflicts:
                logger.warning("{}: {} same-revision rereads changed {}; incoming corrections replace those "
                               "readings, while retained receipts and originals keep their evidence", name, conflicts, digest)

    prior_select = ""
    prior_filter = ""
    if replace_parents is not None:
        scope_columns, scope_values = replace_parents
        if isinstance(scope_columns, str):
            scope_columns = (scope_columns,)
            value_rows = [(value,) for value in scope_values]
        else:
            value_rows = list(scope_values)
        if not scope_columns or len(set(scope_columns)) != len(scope_columns) or any(
            column not in columns or not column.isidentifier() for column in scope_columns
        ):
            raise ValueError("replace_parents must name distinct table columns")
        if any(
            not isinstance(row, tuple) or len(row) != len(scope_columns)
            or any(not isinstance(value, str) for value in row)
            for row in value_rows
        ):
            raise ValueError("replace_parents values must match its columns")
        con.register("replaced_parents", pa.table({
            column: pa.array([row[i] for row in value_rows], type=pa.string())
            for i, column in enumerate(scope_columns)
        }))
        scope_match = " AND ".join(f"r.{column} = p.{column}" for column in scope_columns)
        prior_filter = f"WHERE NOT EXISTS (SELECT 1 FROM replaced_parents r WHERE {scope_match})"
    backfill = dict(backfill_prior or {})
    if any(column not in columns or source not in columns for column, source in backfill.items()):
        raise ValueError("backfill_prior must map table columns to table columns")
    constants = dict(prior_constants or {})
    if not set(constants) <= set(columns):
        raise ValueError("prior_constants must name table columns")
    if have_prior:
        # The prior table may predate columns this contract has since gained —
        # ``congress_bills`` is the live case: its first ten columns are frozen
        # and the bill family appends to them, so a prior published before the
        # family ran has only the ten. Select each missing column as a
        # typed NULL rather than letting the binder fail on it, which makes a
        # column addition a NULL backfill instead of a migration.
        prior_cols = {
            str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{prior_file}')").fetchall()
        }
        if name == "bill_publisher_summaries" and "update_date" in identity:
            # Older tables allowed NULL here. The new revision key must not silently filter those rows out.
            if "update_date" not in prior_cols or con.execute(
                "SELECT EXISTS(SELECT 1 FROM read_parquet(?) WHERE update_date IS NULL)", [str(prior_file)]
            ).fetchone()[0]:
                con.close()
                raise ValueError("CRS summary prior has missing update_date; cannot migrate without losing rows")
        absent = [c for c in columns if c not in prior_cols]
        if absent:
            logger.info(
                "{}: prior table lacks {} of {} columns ({}) — NULL-filling them",
                name,
                len(absent),
                len(columns),
                ", ".join(absent),
            )
        extra = sorted(prior_cols - set(columns))
        if extra:
            # Not fatal — the contract is the published shape — but a column
            # the prior has and the contract does not is either a rename or a
            # removal, and both are worth seeing before the bytes go.
            logger.warning(
                "{}: prior table has {} column(s) the contract does not ({}) — dropping them",
                name,
                len(extra),
                ", ".join(extra),
            )

        def prior_column(column: str) -> str:
            source = backfill.get(column)
            source = source if source in prior_cols else None
            if column in prior_cols:
                return f"COALESCE({column}, {source}) AS {column}" if source else column
            if column in constants:
                return "'" + constants[column].replace("'", "''") + f"' AS {column}"
            return f"{source} AS {column}" if source else f"CAST(NULL AS VARCHAR) AS {column}"

        prior_select = ", ".join(prior_column(c) for c in columns)

    if have_prior and coalesce_prior:
        # Column-wise: a fresh row's NULL must not erase a value the prior
        # row's source stated, so each column takes the fresh value only where
        # the fresh row states one.
        fresh_rank = f"ROW_NUMBER() OVER (PARTITION BY {key_cols} ORDER BY {newest})"
        fresh_q = (
            f"SELECT {cols} FROM ("
            f"SELECT {cols}, 1 AS _src, {fresh_rank} AS _rn FROM read_parquet('{new_file}') WHERE {not_null}"
            f") WHERE _rn = 1"
        )
        prior_q = (f"SELECT * FROM (SELECT {prior_select} FROM read_parquet('{prior_file}') p {prior_filter}) "
                   f"WHERE {not_null}")
        rules = COALESCE_RULES.get(name, {})
        merged = ", ".join(
            c if c in identity else f"{rules[c]} AS {c}" if c in rules else f"COALESCE(f.{c}, p.{c}) AS {c}"
            for c in columns
        )
        selection = f"SELECT {merged} FROM ({fresh_q}) f FULL OUTER JOIN ({prior_q}) p USING ({key_cols})"
    else:
        if have_prior:
            union = (
                f"SELECT {prior_select}, 0 AS _src FROM read_parquet('{prior_file}') p {prior_filter} "
                f"UNION ALL BY NAME "
                f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
            )
        else:
            union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        selection = f"""
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY {key_cols} ORDER BY {newest}
                ) AS _rn
                FROM ({union})
                WHERE {not_null}
            )
            WHERE _rn = 1
        """

    metadata_option = ", KV_METADATA ?" if parquet_metadata else ""
    copy_parameters: list[object] = [str(out_file)]
    if parquet_metadata:
        copy_parameters.append(dict(parquet_metadata))
    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM ({selection})
            ORDER BY {order_by}
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000{metadata_option});
        """,
        copy_parameters,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("{}: {:,} rows", name, total)
    return out_file


#: Tables merged column-wise rather than row-wise, because their rows come
#: from sources that state different subsets of the columns. Only
#: ``congress_bills`` qualifies: the retired list writer (plan A1, decision 31)
#: left the frozen ten on every bill of the archive, ``bill-family`` — now its
#: one writer — fills the whole contract for the Congresses it is scoped to,
#: and a family row that states no ``url`` must not erase the list-era one
#: (decision 1 keeps it, labelled ``inherited``). Adding a table here is a
#: claim that a NULL from one of its sources means "not mine", never "now
#: empty".
COALESCED_TABLES: frozenset[str] = frozenset({"congress_bills"})

#: The two ``update_date`` shapes ``congress_bills`` holds: the list route's date and
#: BILLSTATUS's UTC instant. Measured on the live table of 2026-09-23 (drift audit
#: ``drift-audit-2026-09-23/``): 387,946 dates and 31,920 instants, nothing else, the earliest
#: 2016-10-26 — so a 19xx/20xx year with valid month, day and clock fields is the whole range.
UPDATE_DATE_SHAPE = (
    r"(19|20)\d\d-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ)?"
)

#: Columns a column-wise merge derives instead of coalescing. ``url_source`` names who stated
#: ``url``, so it follows the url the merge keeps: the fresh writer's label when it stated one,
#: ``inherited`` when a fresh row stated none and the prior value survives, and the prior label
#: on a row this run did not touch. Delivery decision 1 (2026-09-22) accepts inherited URLs only
#: with this label. ``update_date`` keeps the larger value, as the contract's column sentence
#: says: a fresh read that states an older stamp than the one published never moves the row
#: backwards, and a same-day date-only value (the list route's) loses to BILLSTATUS's instant,
#: which sorts after it. Only when both sides have one of :data:`UPDATE_DATE_SHAPE`'s two shapes
#: is VARCHAR order time order, so only then is ``GREATEST`` taken; any other value on either
#: side — a ``9999-…`` sentinel, a spaced instant — could never be displaced by it, so the
#: fresh value wins as it does in every other column (a NULL side fails the match too).
COALESCE_RULES: dict[str, dict[str, str]] = {
    "congress_bills": {
        "update_date": (
            f"CASE WHEN regexp_full_match(f.update_date, '{UPDATE_DATE_SHAPE}') "
            f"AND regexp_full_match(p.update_date, '{UPDATE_DATE_SHAPE}') "
            "THEN GREATEST(f.update_date, p.update_date) ELSE COALESCE(f.update_date, p.update_date) END"
        ),
        "url_source": (
            "CASE WHEN f.url IS NOT NULL THEN f.url_source WHEN p.url IS NULL THEN NULL "
            "WHEN f.bill_id IS NULL THEN p.url_source ELSE 'inherited' END"
        ),
    },
}

#: ``congress_bills.statutes_at_large_cite`` is filled from ``laws`` at this
#: table's merge — the contract's own sentence for the column — and from
#: nowhere else: the bill family sees one BILLSTATUS document, and the
#: citation lives in the PLAW USLM file the laws rollup captures once per law.
STATUTES_JOIN_TARGET = "congress_bills"
STATUTES_JOIN_SOURCE = "laws"
STATUTES_JOIN_COLUMN = "statutes_at_large_cite"
STATUTES_JOIN_KEY = "bill_id"


def _duckdb_session(output_dir: Path):
    """A connection with the bounds every merge here runs under: 12 GB, two threads, spilling beside the outputs.

    The dedup window reads the whole prior, which grows with every backfilled Congress: at 4 GB the bill
    family's section_diff_items merge (1.15M prior rows plus the 108th-112th backfill's first 34,723) ran out
    of memory (run 36372044645), as lobbying's prior merge did before 43c1173. Hosted runners have 16 GB;
    this leaves room for Python and the OS, and DuckDB spills the rest to ``temp_directory``.
    """
    import duckdb

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    return con


def _merge_order(version_column: str | None, identity: tuple[str, ...]) -> str:
    """The row order :func:`merge_table` publishes: version descending, then identity."""
    return f"{version_column} DESC, {', '.join(identity)}" if version_column else ", ".join(identity)


def _fill_from_published(
    output_dir: Path,
    out_file: Path,
    *,
    target: str,
    source: str,
    column: str,
    key: str,
    lookup: str,
    order_by: str,
    prefer_row: bool,
    download_prior: Callable[[str, Path], bool],
) -> int:
    """Fill ``column`` on the merged ``target`` at ``out_file`` from the published ``source`` table, best-effort.

    A merge-time join of the kind ``pipelines/rollups/base.py`` permits: ``source`` is an ingest rollup's output,
    read best-effort after the merge so the merge's own semantics are untouched. ``lookup`` is the SQL selecting
    ``key`` and ``column`` from ``read_parquet('{source}')``, one row per key; the two are coalesced, the row's own
    value first when ``prefer_row`` and the lookup's otherwise, so a NULL on one side never clears the other.

    Best-effort end to end, which is the ``soft_inputs`` promise: an absent ``source``, and equally a corrupt,
    truncated or column-short one, leaves ``out_file`` exactly as :func:`merge_table` wrote it, with the failure
    logged, rather than failing the run after its own merge has succeeded. Returns how many rows now carry ``column``.
    """
    import duckdb
    from spicy_docs.transport.credentials import scrub_credential

    source_file = published_table(output_dir, source, download_prior)
    if source_file is None:
        logger.info("{}: no published {} table — {} left as merged", target, source, column)
        return 0
    filled_file = output_dir / f"_{target}_{column}_filled.parquet"
    first, second = ("t", "l") if prefer_row else ("l", "t")
    con = None
    try:
        con = _duckdb_session(output_dir)
        con.execute(
            f"""
            COPY (
                SELECT t.* REPLACE (COALESCE({first}.{column}, {second}.{column}) AS {column})
                FROM read_parquet('{out_file}') t
                LEFT JOIN ({lookup.format(source=source_file)}) l USING ({key})
                ORDER BY {order_by}
            ) TO '{filled_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        counted = con.execute(f"SELECT count(*) FROM read_parquet('{filled_file}') WHERE {column} IS NOT NULL").fetchone()
        count = int(counted[0]) if counted else 0
        filled_file.replace(out_file)
    except (duckdb.Error, OSError) as error:
        logger.warning(
            "{}: joining {} failed — {} left as merged: {}", target, source, column, scrub_credential(str(error), "")
        )
        filled_file.unlink(missing_ok=True)
        return 0
    finally:
        if con is not None:
            con.close()
        source_file.unlink(missing_ok=True)
    logger.info("{}: {:,} rows carry a {} after joining {}", target, count, column, source)
    return count


def fill_statutes_at_large_cite(
    output_dir: Path,
    out_file: Path,
    version_column: str | None,
    identity: tuple[str, ...],
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> int:
    """Fill ``statutes_at_large_cite`` on a merged ``congress_bills`` from the published ``laws`` table.

    ``bill-family``, the writer of ``congress_bills``, declares ``laws`` in ``soft_inputs``. Runs *after* the
    column-wise merge so the coalesce semantics are untouched, and keeps a citation already on the row where
    ``laws`` states none (``laws`` never publishes a captured citation as NULL, so nothing is ever cleared). One law
    per bill: where the route lists two law entries for one bill, the larger ``update_date`` wins, then the key.
    """
    return _fill_from_published(
        output_dir, out_file, target=STATUTES_JOIN_TARGET, source=STATUTES_JOIN_SOURCE, column=STATUTES_JOIN_COLUMN,
        key=STATUTES_JOIN_KEY,
        lookup=f"""SELECT {STATUTES_JOIN_KEY}, {STATUTES_JOIN_COLUMN} FROM read_parquet('{{source}}')
                   WHERE {STATUTES_JOIN_KEY} IS NOT NULL AND {STATUTES_JOIN_COLUMN} IS NOT NULL
                   QUALIFY ROW_NUMBER() OVER (PARTITION BY {STATUTES_JOIN_KEY} ORDER BY update_date DESC, law_id) = 1""",
        order_by=_merge_order(version_column, identity), prefer_row=False, download_prior=download_prior,
    )


def fill_senate_bioguide_ids(output_dir: Path, out_file: Path, download_prior: Callable[[str, Path], bool] = r2.download) -> int:
    """Fill ``bioguide_id`` on a merged ``member_votes`` from the published ``members`` table, through the LIS id.

    A Senate file names a member by LIS id alone, and ``member_votes.bioguide_id`` promises the crosswalk's id;
    ``members``, the community crosswalk's ingest output, is where a LIS id resolves (``member_vote_terms`` reads the
    column this fills). Only a NULL ``bioguide_id`` whose ``lis_id`` exactly one member carries is filled: a House row
    states no LIS id, a bioguide id the file itself states stands (spicy-docs' ``match_member`` order), and an unknown
    or shared LIS id is left as the file stated it. Best-effort: with no ``members`` published, every Senate row stays
    NULL and ``member_vote_terms`` reads it as ``unresolved_member``, which its counts show.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    contract = TABLE_CONTRACTS["member_votes"]
    return _fill_from_published(
        output_dir, out_file, target=contract.name, source="members", column="bioguide_id", key="lis_id",
        lookup="""SELECT lis_id, bioguide_id FROM read_parquet('{source}')
                  WHERE lis_id IS NOT NULL AND bioguide_id IS NOT NULL
                  QUALIFY count(*) OVER (PARTITION BY lis_id) = 1""",
        order_by=_merge_order(contract.version_column, contract.identity), prefer_row=True,
        download_prior=download_prior,
    )


def merge_contract_table(
    output_dir: Path,
    contract_name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
    replace_parents: ReplacementScope | None = None,
    backfill_prior: Mapping[str, str] | None = None,
    parquet_metadata: Mapping[str, str] | None = None,
) -> Path:
    """Merge ``rows`` for one ``spicy_docs.schemas`` table contract.

    The single door the twenty-two hosted tables go through. The contract owns
    the column tuple, the identity and the version column, so this repository
    never restates a published shape: a column appended in spicy-docs appears
    here by re-reading ``TABLE_CONTRACTS``, and the NULL-fill in
    :func:`merge_table` makes that a backfill rather than a migration.

    The remote key is ``{contract_name}.parquet``, matching the R2 key = MCP
    view = dictionary key convention every other published table follows.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    contract = TABLE_CONTRACTS[contract_name]
    coalesce = contract_name in COALESCED_TABLES
    out_file = merge_table(
        output_dir,
        name=contract.name,
        columns=contract.columns,
        identity=contract.identity,
        version_column=contract.version_column,
        rows=rows,
        remote_key=f"{contract.name}.parquet",
        download_prior=download_prior,
        prior_present=prior_present,
        coalesce_prior=coalesce,
        replace_parents=replace_parents,
        backfill_prior=backfill_prior,
        parquet_metadata=parquet_metadata,
    )
    if contract_name == STATUTES_JOIN_TARGET:
        fill_statutes_at_large_cite(output_dir, out_file, contract.version_column, contract.identity, download_prior)
    return out_file


def retire_prior_rows(prior_file: Path, **scope: str) -> int:
    """Drop from a downloaded prior table every row matching all of ``scope``, returning how many went.

    For a table that is a *snapshot* rather than an accumulation — today's
    committee seats, one session's classification page — a row the publisher
    no longer lists must not linger from an earlier capture, and
    :func:`merge_table` alone keeps every prior row. Rewriting the prior
    scratch file without the scope's rows before the merge makes that merge
    "replace the scope", and leaves every other scope's rows standing (an
    earlier Congress keeps its last capture). ``scope`` names columns of the
    prior table; values are bound as parameters, and an empty scope or a
    non-snake_case column refuses.
    """
    import duckdb

    if not scope:
        raise ValueError("retire_prior_rows needs at least one scope column")
    for column in scope:
        if not column.isidentifier() or column != column.lower():
            raise ValueError(f"scope column {column!r} is not a snake_case identifier")
    where = " AND ".join(f"{column} = ?" for column in scope)
    values = list(scope.values())
    kept_file = prior_file.with_name(prior_file.stem + "_kept.parquet")
    con = duckdb.connect()
    retired = con.execute(f"SELECT count(*) FROM read_parquet('{prior_file}') WHERE {where}", values).fetchone()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{prior_file}') WHERE NOT ({where}) OR ({' OR '.join(f'{c} IS NULL' for c in scope)}))"
        f" TO '{kept_file}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        values,
    )
    con.close()
    kept_file.replace(prior_file)
    count = int(retired[0]) if retired else 0
    logger.info("{}: retired {:,} prior row(s) for {}", prior_file.name, count, scope)
    return count
