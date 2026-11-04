"""A bounded admitted family reader owned by one explicit conversion operation.

Ordinary table readers keep their independent, narrow admission. This lifetime
reuses the same maintained admission and matching replay for complete conversions.
"""
from contextlib import ExitStack, closing, contextmanager
import json
from itertools import islice
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from time import perf_counter

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    _bundle_policies, _index_processing, _joined_subjects, _load_receipts, _unpack, select_receipts,
)
from spicy_regs.runtime_bounds import checkpoint


_GROUP_ROWS, _GROUP_BYTES = 16_384, 64 * 1024**2


def _grouped(batches):
    """Concatenate ordered record batches into tables of bounded rows and bytes, so one query spans many chunks."""
    import pyarrow as pa

    group, rows, size = [], 0, 0
    for batch in batches:
        if group and (rows + batch.num_rows > _GROUP_ROWS or size + batch.nbytes > _GROUP_BYTES):
            yield pa.Table.from_batches(group)
            group, rows, size = [], 0, 0
        group.append(batch)
        rows, size = rows + batch.num_rows, size + batch.nbytes
    if group:
        yield pa.Table.from_batches(group)


class _FamilyReader:
    def __init__(self, connection, subjects, policies, guard, *, row_reference):
        self.connection, self.subjects, self.policies = connection, subjects, policies
        self.guard, self.row_reference = guard, row_reference
        self.active, self.reading = True, False
        self.admission_seconds = 0.0

    def check(self):
        checkpoint()
        self._require_active()
        self.guard()

    def _require_active(self):
        if not self.active:
            raise ValueError("Conversion family reader is closed")

    def _live_rows(self, rows):
        iterator = iter(rows)
        try:
            while True:
                self._require_active()
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                self._require_active()
                yield row
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                try:
                    close()
                except (etl_bulk.duckdb.Error, sqlite3.Error):
                    if self.active:
                        raise

    @contextmanager
    def _stream(self):
        self.check()
        if self.reading:
            raise ValueError("Finish the active family stream before another read")
        self.reading = True
        try:
            yield
            self.check()
        finally:
            self.reading = False

    def read_subjects(self, dataset):
        """Exercise the shared exact matcher using this operation's admitted state."""
        policy, paths = self.policies[dataset], self.subjects[dataset]
        with self._stream():
            if self.row_reference:
                self.connection.execute("UPDATE receipts SET used=0 WHERE dataset=?", [dataset])
                try:
                    for _, subject, processing in self._live_rows(_joined_subjects(
                            self.connection, {dataset: paths}, {dataset: policy})):
                        yield subject | processing
                finally:
                    if self.active:
                        # Admission already proved every dataset. A cancelled
                        # replay must not leave unrelated reads with unused flags.
                        self.connection.execute("UPDATE receipts SET used=1 WHERE dataset=?", [dataset])
            else:
                first = self.connection.execute(
                    "SELECT coalesce(min(n),0) FROM subjects WHERE dataset=?", [dataset]).fetchone()[0]
                yield from self._live_rows(etl_bulk._replay_admitted_subjects(self.connection, paths, policy, start=first))

    def processing(self, dataset, *, outcomes=None):
        """Visit admitted original processing fields in receipt order without rereading Parquet."""
        if dataset not in self.policies:
            raise ValueError("Dataset is outside the admitted conversion family")
        with self._stream():
            if self.row_reference:
                rows = self.connection.execute(
                    "SELECT outcome,processing,diagnostics FROM receipts WHERE dataset=? ORDER BY rowid", [dataset])
                for outcome, processing, diagnostics in self._live_rows(rows):
                    checkpoint()
                    if outcomes is None or outcome in outcomes:
                        yield {"outcome": outcome, "processing_fields": _index_processing(processing),
                               "diagnostics": _index_processing(diagnostics)}
            else:
                query = "SELECT outcome,processing_json,diagnostic_json FROM receipts WHERE dataset=? ORDER BY n"
                for batch in self._live_rows(self.connection.execute(query, [dataset]).to_arrow_reader(etl_bulk._BATCH)):
                    checkpoint()
                    for outcome, processing, diagnostics in zip(*(batch.column(i).to_pylist() for i in range(3)), strict=True):
                        self._require_active()
                        if outcomes is None or outcome in outcomes:
                            yield {"outcome": outcome, "processing_fields": _unpack(json.loads(processing)),
                                   "diagnostics": _unpack(json.loads(diagnostics))}

    def outcome_counts(self):
        """Count actual admitted receipt rows without decoding the source log again."""
        with self._stream():
            return {outcome: count for outcome, count in self.connection.execute(
                "SELECT outcome,count(*) FROM receipts GROUP BY outcome").fetchall()}

    def compare_original(self, dataset, original, *, source_schema=None):
        """Prove exact source members, Arrow schema/metadata, values, order and repetitions."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from spicy_regs.congress_subjects import INPUT_COLUMNS as congress
        from spicy_regs.legislative_documents import field_registry
        from spicy_regs.local_data import file_signature
        from spicy_regs.native_types import extra_field_checker

        self.check()
        original = Path(original)
        if original.is_symlink():
            raise ValueError("Retained original must not be a symlink")
        members = ({p.relative_to(original).as_posix(): p for p in sorted(original.rglob('*.parquet'))}
                   if original.is_dir() else {original.name: original})
        before = {name: file_signature(path) for name, path in members.items()}

        def compare(path, schema, rows):
            with pq.ParquetFile(path) as parquet:
                if not schema.equals(parquet.schema_arrow, check_metadata=True):
                    raise ValueError("Retained original schema or metadata changed")
                iterator = iter(rows)
                check = extra_field_checker(schema)
                for expected in parquet.iter_batches(batch_size=2000):
                    checkpoint()
                    batch = list(islice(iterator, expected.num_rows))
                    for row in batch:
                        check(row)
                    actual = pa.RecordBatch.from_pylist(batch, schema=schema)
                    if not expected.equals(actual):
                        raise ValueError("Restored processing values, order or repetitions differ from original")
                if next(iterator, None) is not None:
                    raise ValueError("Extra restored processing rows")

        if dataset in field_registry():
            from spicy_regs.legislative_receipts import FILE_STATES, retained_source_rows
            policy = self.policies[dataset]
            states = [r['processing_fields']['file_state'] for r in self.processing(
                FILE_STATES, outcomes=frozenset({'observed'}))
                if r['processing_fields']['file_state']['dataset'] == dataset]
            if not states or len({s['relative_path'] for s in states}) != len(states):
                raise ValueError("Missing or duplicate retained source file state")
            if states[0]['relative_path'] is None:
                if len(states) != 1 or not states[0]['partitioned'] or states[0]['rows'] != 0 or members or not original.is_dir():
                    raise ValueError("Invalid empty partitioned source state")
            elif any(s['partitioned'] != original.is_dir() for s in states):
                raise ValueError("Retained table physical membership changed")
            named = {s['relative_path'] if s['partitioned'] else original.name for s in states
                     if s['relative_path'] is not None}
            if named != set(members):
                raise ValueError("Retained table member set changed")
            records = ((r['processing_fields'] for r in self.processing(dataset, outcomes=frozenset({'observed'})))
                       if policy.receipt_only else self.read_subjects(dataset))
            rows = iter(retained_source_rows(dataset, policy, records))
            for state in states:
                if state['relative_path'] is None:
                    continue
                relative = Path(state['relative_path'])
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError("Invalid retained member path")
                name = state['relative_path'] if state['partitioned'] else original.name
                path = members[name]
                if pq.read_metadata(path).num_rows != state['rows']:
                    raise ValueError("Retained source member row count changed")
                schema = pa.ipc.read_schema(pa.BufferReader(state['schema']))
                compare(path, schema, islice(rows, state['rows']))
            if next(rows, None) is not None:
                raise ValueError("Extra prior receipt inputs without retained source rows")
        elif dataset.startswith('fec_'):
            from spicy_regs.transforms.fec_native_subjects import FIELD_RULES
            from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
            if dataset not in FIELD_RULES and dataset not in REGISTRY:
                raise ValueError("No maintained original decoder for admitted FEC dataset")
            policy = self.policies[dataset]
            records = ((r['processing_fields'] for r in self.processing(dataset, outcomes=frozenset({'observed'})))
                       if policy.receipt_only else self.read_subjects(dataset))
            source_columns = pq.read_schema(next(iter(members.values()))).names
            def originals():
                for row in records:
                    if dataset in FIELD_RULES:
                        moved = row['fec_conversion_inputs']
                        yield {name: moved[name] if name in moved else row[name]
                               for name in row['fec_input_columns']}
                    else:
                        moved = row['conversion_inputs']
                        overrides = (row.get('source_input') or {}).get('fec_retained_overrides', {})
                        yield {name: overrides.get(name, moved.get(name, row.get(name)))
                               for name in source_columns}
            rows = iter(originals())
            for path in members.values():
                count = pq.ParquetFile(path).metadata.num_rows
                compare(path, pq.read_schema(path), islice(rows, count))
            if next(rows, None) is not None:
                raise ValueError("Extra restored FEC processing rows")
        else:
            if original.is_dir():
                raise ValueError("This source requires a single retained Parquet member")
            schema = pq.read_schema(original)
            if dataset in congress:
                from spicy_regs.congress_receipts import retained_source_rows
                # Exercise exact subject matching once before the retained source stream.
                for _ in self.read_subjects(dataset):
                    pass
                rows = retained_source_rows(self.processing(dataset), schema=schema)
            elif dataset.startswith('court_'):
                from spicy_regs.court_receipts import processing_rows
                schema = schema.remove_metadata()
                with pq.ParquetFile(original) as parquet:
                    if not schema.equals(parquet.schema_arrow, check_metadata=True):
                        raise ValueError("Retained original schema or metadata changed")
                # A bulk proof only ever accepts. Anything it cannot prove goes to the row comparison below.
                if self._bulk_court_equal(dataset, original, schema):
                    rows = None
                else:
                    rows = processing_rows(self.read_subjects(dataset), dataset=dataset, schema=schema)
            else:
                from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS
                if dataset in SOURCE_COLUMNS:
                    self._regulatory_original_schema(dataset, schema, source_schema=source_schema)
                    rows = (None if self._bulk_regulatory_equal(dataset, original, schema) else
                            self._regulatory_original_rows(dataset, schema, source_schema=source_schema))
                else:
                    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
                    if dataset not in REGISTRY:
                        raise ValueError("No maintained original decoder for admitted dataset")
                    def fec_rows():
                        for row in self.read_subjects(dataset):
                            originals = row['conversion_inputs']
                            yield {name: originals.get(name, row.get(name)) for name in schema.names}
                    rows = fec_rows()
            if rows is not None:
                compare(original, schema, rows)
        if {name: file_signature(path) for name, path in members.items()} != before:
            raise ValueError("Retained original changed during comparison")
        if original.is_dir() and set(p.relative_to(original).as_posix() for p in original.rglob('*.parquet')) != set(members):
            raise ValueError("Retained original member set changed during comparison")
        self.check()
        checked = _equal_original_report()
        if original.is_dir():
            checked.update(members={name: _equal_original_report() | {'schema_changed': False} for name in members},
                           members_only_in_restored=[], members_only_in_retained=[], schema_changes=[])
        return checked

    def _bulk_court_equal(self, dataset, original, schema):
        """True only when SQL proves every restored court row equals its retained original row.

        The row path lets ``conversion_inputs`` override the receipt's processing fields, which override the
        subject, and keeps the declared columns. Here each column's exact encoding (``exact_json_sql``, equal to
        Python's ``exact_json``) is compared, ordinal by ordinal, with the encoding stored in the receipt or
        computed from the subject, so equal text means equal values, order and repetitions. Subject identity is
        re-derived as the replay does. Only rows SQL cannot prove go through the maintained Python decoder. If
        that subset differs, the full row comparison supplies its original refusal message.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        policy, paths = self.policies[dataset], self.subjects[dataset]
        if self.row_reference or policy.receipt_only or len(paths) != 1:
            return False
        number = "n"
        while number in {field.name.casefold() for field in policy.subject_schema}:
            number += "_"
        subject_types = {field.name: field.type for field in policy.subject_schema}
        record_id, version, identity = etl_bulk.identity_sql(policy)
        derived = [(record_id, "_btv_id"), (version, "_btv_version"), (identity, "_btv_identity"),
                   (etl_bulk._subject_reference_sql(policy), "_btv_reference")]
        ready = ("NOT _btv_unproven AND NOT _btv_receipt_unproven AND _btv_reference IS FALSE AND _btv_id = _btv_rid"
                 " AND _btv_version = _btv_ver AND _btv_identity = _btv_ident")
        equal = []
        try:
            for index, field in enumerate(schema):
                key = etl_bulk._literal(field.name)
                if field.name in subject_types:
                    absent = etl_bulk.exact_json_sql('"' + field.name.replace('"', '""') + '"', subject_types[field.name])
                elif dataset == 'court_dockets' and field.name in ('parties_json', 'attorneys_json', 'firms_json'):
                    absent = "NULL"  # The row path builds these from the unserialized column; only it may.
                else:
                    absent = "'[\"null\",null]'"
                restored = f"coalesce(_btv_ci[{key}], _btv_top[{key}], {absent})"
                if dataset == 'court_opinion_clusters' and field.name == 'attorneys':
                    # The row path wraps a bare string in a list.
                    restored = f"CASE WHEN {restored} LIKE '[\"str\",%' THEN NULL ELSE {restored} END"
                text = f"coalesce(({restored}) = {etl_bulk.exact_json_sql(f'"__o{index}"', field.type)}, false)"
                declared = subject_types.get(field.name)
                if (declared is not None and declared.equals(field.type) and not pa.types.is_floating(field.type)
                        and (dataset, field.name) != ('court_opinion_clusters', 'attorneys')):
                    # Receipt fields never share a subject column's name, so only conversion_inputs can override it.
                    column = '"' + field.name.replace('"', '""') + '"'
                    text = (f'CASE WHEN _btv_ci[{key}] IS NULL THEN {column} IS NOT DISTINCT FROM "__o{index}"'
                            f" ELSE {text} END")
                equal.append(text)
        except NotImplementedError:
            return False

        def entries(text):
            return ("map_from_entries(list_transform(CAST(json_extract(" + text + ", '$[1]') AS JSON[]),"
                    " e -> {'k': e->>'$[0]', 'v': CAST(json_extract(e, '$[1]') AS VARCHAR)}))")

        conversion = "_btv_top['conversion_inputs']"
        query = (
            "WITH s AS (SELECT *, " + ", ".join(f"{expression} AS {alias}" for expression, alias in derived)
            + " FROM btv_subjects), j AS (SELECT s.*, btv_originals.*, d.unproven AS _btv_unproven,"
            " r.unproven AS _btv_receipt_unproven, d.record_id AS _btv_rid, d.version AS _btv_ver,"
            " d.identity AS _btv_ident, r.processing_json AS _btv_pj"
            f' FROM s JOIN btv_originals ON s."{number}" = btv_originals.__row'
            f' JOIN (SELECT * FROM subjects WHERE NOT unproven AND n >= ? AND n < ?) d ON d.n = s."{number}"'
            " JOIN (SELECT * FROM subject_receipts WHERE n >= ? AND n < ?) m ON m.n = d.n"
            " JOIN receipts r ON r.n = m.receipt),"
            " t AS (SELECT *, " + entries("_btv_pj") + " AS _btv_top FROM j),"
            f" c AS (SELECT *, CASE WHEN {conversion} LIKE '[\"dict\",%' THEN " + entries(conversion)
            + " ELSE map_from_entries([]::STRUCT(k VARCHAR, v VARCHAR)[]) END AS _btv_ci FROM t)"
            " SELECT count(*), list(struct_pack(ordinal := __row, processing := _btv_pj,"
            " record_id := _btv_rid, version := _btv_ver, identity := _btv_ident)) FILTER (WHERE NOT coalesce("
            + ready + " AND (" + conversion + " IS NULL OR " + conversion + " = '[\"null\",null]' OR "
            + conversion + " LIKE '[\"dict\",%') AND " + " AND ".join(equal) + ", false)) FROM c")
        first = self.connection.execute(
            "SELECT coalesce(min(n),0) FROM subjects WHERE dataset=?", [dataset]).fetchone()[0]
        count = 0
        with self._stream(), etl_bulk._parquet(paths[0]) as parquet, pq.ParquetFile(original) as retained:
            if (not parquet.schema_arrow.equals(policy.subject_schema)
                    or parquet.metadata.num_rows != retained.metadata.num_rows):
                return False
            held, pending, read = retained.iter_batches(batch_size=etl_bulk._BATCH), [], [0, 0]
            try:
                with closing(etl_bulk._numbered(parquet, first, number, None, read)) as numbered:
                    for subjects in _grouped(numbered):
                        checkpoint()
                        # Cut the original into the same row ranges, one ordinal per row.
                        while sum(piece.num_rows for piece in pending) < subjects.num_rows and (
                                more := next(held, None)):
                            pending.append(more)
                        table = pa.Table.from_batches(pending, schema=retained.schema_arrow)
                        if table.num_rows < subjects.num_rows:
                            return False
                        pending = table.slice(subjects.num_rows).to_batches()
                        low, high = first + count, first + count + subjects.num_rows
                        originals = pa.table(
                            [*table.slice(0, subjects.num_rows).columns, pa.array(range(low, high), pa.int64())],
                            names=[*(f"__o{i}" for i in range(len(schema))), "__row"])
                        self.connection.register("btv_subjects", subjects)
                        self.connection.register("btv_originals", originals)
                        try:
                            total, unproven = self.connection.execute(query, [low, high, low, high]).fetchone()
                        finally:
                            self.connection.unregister("btv_subjects")
                            self.connection.unregister("btv_originals")
                        if total != subjects.num_rows:
                            return False
                        if unproven and not self._court_subset_equal(
                                dataset, schema, subjects, table, low, unproven):
                            return False
                        count += subjects.num_rows
            except (etl_bulk.duckdb.Error, pa.ArrowException) as error:
                raise etl_bulk.bulk_failure('Court comparison proof failed', error) from error
            return (count == parquet.metadata.num_rows and not any(piece.num_rows for piece in pending)
                    and next(held, None) is None and read == [count, count])

    def _court_subset_equal(self, dataset, schema, subjects, originals, first, unproven):
        """Replay only the bounded batch's unproven ordinals, with the same subject binding as full replay."""
        import pyarrow as pa
        from spicy_regs.court_receipts import processing_rows
        from spicy_regs.etl_receipts import subject_identity

        policy = self.policies[dataset]
        positions = [row['ordinal'] - first for row in unproven]
        selected = subjects.take(positions).select(policy.subject_schema.names).to_pylist()
        rows = []
        for subject, receipt in zip(selected, unproven, strict=True):
            if subject_identity(policy, subject) != (receipt['record_id'], receipt['version'], receipt['identity']):
                return False
            rows.append(subject | _unpack(json.loads(receipt['processing'])))
        try:
            restored = pa.Table.from_pylist(list(processing_rows(rows, dataset=dataset, schema=schema)), schema=schema)
            return originals.take(positions).equals(restored)
        except (ValueError, TypeError, KeyError):
            return False

    def _regulatory_original_schema(self, dataset, schema, *, source_schema=None):
        from spicy_regs.transforms.regulations_receipts import processing_schema
        from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES

        known = dict(SOURCE_COLUMNS[dataset])
        if len(set(schema.names)) != len(schema.names) or any(
                f.name not in known or f.type != TYPES[known[f.name]] for f in schema):
            raise ValueError("Exact regulatory source schema differs from declared input fields")
        expected_metadata = schema.metadata or {}
        restored_schema = processing_schema(dataset, source_schema=source_schema, metadata=expected_metadata)
        if not restored_schema.equals(schema, check_metadata=True):
            raise ValueError("Maintained regulatory restored schema differs from retained original")
        return expected_metadata

    def _bulk_regulatory_equal(self, dataset, original, schema):
        """Prove original equality with the maintained regulatory restore query and sparse row replay.

        This reuses the admitted ordinal map and R's raw-input/mapper proof. Every held subject is bound again to
        its admitted identity and version. Plain rows compare as Arrow values; only unproven rows run the maintained
        Python mapper. A failed proof returns to the complete comparator for its original refusal.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        from spicy_regs import regulations_bulk
        from spicy_regs.etl_receipts import exact_json
        from spicy_regs.transforms.regulations_receipts import policy as regulatory_policy

        policy, paths = self.policies[dataset], self.subjects[dataset]
        if (self.row_reference or dataset not in regulations_bulk.DATASETS or len(paths) != 1
                or policy.descriptor() != regulatory_policy(dataset).descriptor()):
            return False
        try:
            plain = regulations_bulk._plain_query(dataset, policy, schema)
            if plain is None:
                return False
            metadata = exact_json({key.decode('utf-8'): value for key, value in (schema.metadata or {}).items()})
            record_id, version, identity = etl_bulk.identity_sql(policy)
            reference = etl_bulk._subject_reference_sql(policy)
        except (NotImplementedError, UnicodeError):
            return False
        number = 'n'
        while number in {field.name.casefold() for field in policy.subject_schema}:
            number += '_'
        query = (
            f'SELECT s."{number}" AS _i, r.identity_json, r.processing_json, '
            'CASE WHEN s._reference IS FALSE THEN s._id=d.record_id AND s._version=d.version '
            'AND s._identity=d.identity END AS _bound, d.record_id, d.version, d.identity '
            f'FROM (SELECT *, {record_id} AS _id, {version} AS _version, {identity} AS _identity, '
            f'{reference} AS _reference FROM btv_regulatory_subjects) s '
            f'JOIN (SELECT * FROM subjects WHERE NOT unproven AND n>=? AND n<?) d ON d.n=s."{number}" '
            'JOIN (SELECT * FROM subject_receipts WHERE n>=? AND n<?) m ON m.n=d.n '
            f'JOIN receipts r ON r.n=m.receipt ORDER BY s."{number}"')
        first = self.connection.execute('SELECT coalesce(min(n),0) FROM subjects WHERE dataset=?', [dataset]).fetchone()[0]
        count, functions = 0, []
        with self._stream(), etl_bulk._parquet(paths[0]) as parquet, pq.ParquetFile(original) as retained:
            if (not parquet.schema_arrow.equals(policy.subject_schema) or not parquet.metadata.num_rows
                    or parquet.metadata.num_rows != retained.metadata.num_rows):
                return False  # Empty files also require the maintained observation-metadata check.
            pending, read = [], [0, 0]
            originals = retained.iter_batches(batch_size=etl_bulk._BATCH)
            try:
                for name, dtype in regulations_bulk._subject_sql(dataset, policy, schema)[0]:
                    self.connection.create_function(f'parse_{name}', regulations_bulk._parser(dataset, name, dtype),
                        ['VARCHAR'], 'STRUCT(native VARCHAR, reference BOOLEAN)', type='arrow', null_handling='special')
                    functions.append(f'parse_{name}')
                with closing(etl_bulk._numbered(parquet, first, number, None, read)) as numbered:
                    for subjects in _grouped(numbered):
                        checkpoint()
                        while sum(piece.num_rows for piece in pending) < subjects.num_rows and (more := next(originals, None)):
                            pending.append(more)
                        table = pa.Table.from_batches(pending, schema=retained.schema_arrow)
                        if table.num_rows < subjects.num_rows:
                            return False
                        pending = table.slice(subjects.num_rows).to_batches()
                        table = table.slice(0, subjects.num_rows)
                        low, high = first + count, first + count + subjects.num_rows
                        index = pa.array(range(low, high), pa.int64())
                        self.connection.register('btv_regulatory_subjects', subjects)
                        try:
                            receipts = self.connection.execute(query, [low, high, low, high]).to_arrow_table()
                        finally:
                            self.connection.unregister('btv_regulatory_subjects')
                        if not receipts['_i'].combine_chunks().equals(index):
                            return False
                        subject = subjects.select(policy.subject_schema.names)
                        self.connection.register('s', subject.rename_columns([f'_s_{name}' for name in subject.column_names])
                                                 .append_column('_i', index))
                        self.connection.register('r', receipts.select(['_i', 'identity_json', 'processing_json']))
                        try:
                            result = self.connection.execute(plain).to_arrow_table()
                        finally:
                            self.connection.unregister('s')
                            self.connection.unregister('r')
                        if not result['_i'].combine_chunks().equals(index):
                            return False
                        flags = [bound is True and identity_ok and proven and meta == metadata
                                 for bound, identity_ok, proven, meta in zip(receipts['_bound'].to_pylist(),
                                 result['_identity_ok'].to_pylist(), result['_plain'].to_pylist(),
                                 result['_meta'].to_pylist(), strict=True)]
                        mask = pa.array(flags, pa.bool_())
                        if not result.filter(mask).select(schema.names).cast(schema).equals(table.filter(mask)):
                            return False
                        rest = [i for i, flag in enumerate(flags) if not flag]
                        if rest and not self._regulatory_subset_equal(dataset, schema, subject.take(rest),
                                                                     receipts.take(rest), table.take(rest)):
                            return False
                        count += subjects.num_rows
            except (etl_bulk.duckdb.Error, pa.ArrowException) as error:
                raise etl_bulk.bulk_failure('Regulatory comparison proof failed', error) from error
            finally:
                for name in functions:
                    self.connection.remove_function(name)
            return (count == parquet.metadata.num_rows and not any(piece.num_rows for piece in pending)
                    and next(originals, None) is None and read == [count, count])

    def _regulatory_subset_equal(self, dataset, schema, subjects, receipts, original):
        import pyarrow as pa
        from spicy_regs.etl_receipts import subject_identity
        from spicy_regs.transforms.regulations_receipts import _processor_inputs

        try:
            rows = []
            for subject, receipt in zip(subjects.to_pylist(), receipts.to_pylist(), strict=True):
                if subject_identity(self.policies[dataset], subject) != (
                        receipt['record_id'], receipt['version'], receipt['identity']):
                    return False
                row = subject | _unpack(json.loads(receipt['processing_json']))
                if (pa.schema([], metadata=row.get('input_metadata', {})).metadata or {}) != (schema.metadata or {}):
                    return False
                rows.append(row)
            raw = _processor_inputs(dataset, rows)
            if any(set(row) != set(schema.names) for row in raw):
                return False
            return pa.Table.from_pylist(raw, schema=schema).equals(original)
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
            return False

    def _regulatory_original_rows(self, dataset, schema, *, source_schema=None):
        import pyarrow as pa
        from spicy_regs.transforms.regulations_receipts import _processor_inputs

        expected_metadata = self._regulatory_original_schema(dataset, schema, source_schema=source_schema)
        rows = iter(self.read_subjects(dataset))
        count = 0
        while batch := list(islice(rows, 2000)):
            for row in batch:
                if (pa.schema([], metadata=row.get('input_metadata', {})).metadata or {}) != expected_metadata:
                    raise ValueError("Regulatory input metadata differs from retained original")
            for raw in _processor_inputs(dataset, batch):
                if set(raw) != set(schema.names):
                    raise ValueError("Exact regulatory source field presence differs from retained original")
                count += 1
                yield raw
        if not count:
            held = None
            for attempt in self.processing(dataset, outcomes=frozenset({'observed'})):
                if attempt['diagnostics'].get('kind') == 'input_file_metadata':
                    metadata = pa.schema([], metadata=attempt['processing_fields']['input_metadata']).metadata or {}
                    if held is not None and held != metadata:
                        raise ValueError("Empty input metadata differs across selected receipts")
                    held = metadata
            if (held or {}) != expected_metadata:
                raise ValueError("Empty regulatory input metadata differs from retained original")


def _equal_original_report():
    """Ordered exact equality implies no bag, column, type or metadata differences."""
    return {'metadata_changes': [], 'columns_only_in_restored': [], 'columns_only_in_retained': [],
            'type_changes': {}, 'rows_only_in_restored': 0, 'rows_only_in_retained': 0}


@contextmanager
def _open_family_reader(subjects, receipts, policies, *, generation_id, guard, scoped=False):
    """Admit all supplied family inputs once, retain bounded processing, then close on every exit.

    The owning conversion supplies an immutable-source change guard. It also
    checks complete descriptor/membership agreement before reaching this helper.
    No saved passed flag or validation bypass can create this reader.
    A scoped single-dataset read selects shared receipts during bulk admission;
    only its reference fallback needs a separate selected file.
    """
    registered = _bundle_policies(subjects, receipts, policies)
    if scoped and len(registered) != 1:
        raise ValueError("Scoped conversion reads require one selected dataset")
    guard()
    start = perf_counter()
    with ExitStack() as stack:
        try:
            connection = stack.enter_context(etl_bulk._validated_bundle(
                subjects, receipts, policies, generation_id=generation_id, retain_processing=True, scoped=scoped))
            reference = False
        except etl_bulk.NotBulkEligible as error:
            etl_bulk.log_row_fallback("conversion admission", error, dataset=",".join(subjects))
            temporary = Path(stack.enter_context(TemporaryDirectory(prefix="conversion-reference-")))
            connection = stack.enter_context(closing(sqlite3.connect(str(temporary / "joins.db"))))
            # The reference index has the same finite disk ceiling as bulk spill.
            page_size = connection.execute("PRAGMA page_size").fetchone()[0]
            connection.execute(f"PRAGMA max_page_count={32 * 1024**3 // page_size}")
            selected = receipts
            if scoped:
                selected = [select_receipts(path, temporary / f"receipts-{i}.parquet", dataset=next(iter(registered)))
                            for i, path in enumerate(receipts)]
            _load_receipts(connection, selected, registered, generation_id, retain_diagnostics=True)
            for _ in _joined_subjects(connection, subjects, registered):
                pass
            reference = True
        guard()
        reader = _FamilyReader(connection, subjects, registered, guard, row_reference=reference)
        reader.admission_seconds = perf_counter() - start
        try:
            yield reader
            reader.check()
        finally:
            reader.active = False


class ConversionReadOperation:
    """Keep actual admitted generation state until this explicit operation closes.

    Build, full original comparisons, same-process publication and the public
    byte read-back share this lifetime. A restart creates a new operation.
    """
    def __init__(self):
        self._stack = ExitStack()
        self._generations = {}
        self._guards = {}
        self._reader_stacks = {}
        self._policies = {}
        self.active = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *error):
        try:
            return self._stack.__exit__(*error)
        finally:
            self.active = False

    def admit_generation(self, directory, *, expected_pin=None, semantic=True):
        """Hash every member once and retain the same product receipt admission.

        ``semantic=False`` is the sealed-publication admission: every byte, structural and installed-policy check
        still runs, but no family reader opens (no bundle validation, no row joins), and the held reader is ``None``.
        Only a caller whose sealed semantic admission matches this process's ``admission_key`` may ask for it.
        """
        from rulespec_artifacts import ArtifactPin, LocalFileState, LocalMemberSource
        from spicy_regs.generations import _verify_generation_source
        from spicy_regs.local_data import file_signature

        if not self.active:
            raise ValueError("Conversion read operation is closed")
        if isinstance(expected_pin, dict):
            if set(expected_pin) != {'logicalId', 'artifactDigest'}:
                raise ValueError("Invalid expected conversion artifact pin")
            expected_pin = ArtifactPin(expected_pin['logicalId'], expected_pin['artifactDigest'])
        directory = Path(directory).absolute()
        held = self._generations.get(directory)
        if held is not None:
            artifact, reader, source = held
            if reader is None and semantic:
                raise ValueError("Conversion generation was admitted by bytes only; its rows were not admitted here")
            if reader is not None:
                reader.check()
            if expected_pin is not None and artifact.pin != expected_pin:
                raise ValueError("Conversion generation differs from its expected pin")
            return held
        source = LocalMemberSource(directory)
        keys = tuple(source.keys())
        signatures = {key: file_signature(directory / key) for key in keys}

        def guard():
            if not self.active:
                raise ValueError("Conversion read operation is closed")
            if set(source.keys()) != set(keys) or any(
                    file_signature(directory / key) != signature for key, signature in signatures.items()):
                raise ValueError("Conversion generation member changed after admission")

        def footer(key):
            # Complete receipt/subject admission decodes every data page below.
            # Repeating that decode merely to count rows is unnecessary here.
            import duckdb
            import pyarrow.parquet as pq
            with source.open(key) as stream:
                parquet = pq.ParquetFile(stream)
                rows = parquet.metadata.num_rows
            with duckdb.connect() as connection:
                columns = connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                    [str(directory / key)]).fetchall()
            return {"columns": [[name, kind] for name, kind, *_ in columns], "rows": rows}

        readers, registered = [], []
        reader_stack = ExitStack()
        self._stack.enter_context(reader_stack)

        def admit(subjects, receipts, policies, *, generation_id):
            from spicy_regs.etl_receipts import receipt_policies
            from spicy_regs.subject_catalog import policies as installed_policies

            installed = installed_policies()
            for policy in policies:
                current = installed.get(policy.dataset)
                if current is None or not any(
                        policy.descriptor() == candidate.descriptor() for candidate in receipt_policies(current)):
                    raise ValueError("Conversion generation policy descriptor is not an exact shipped policy")
            registered.append(_bundle_policies(subjects, receipts, policies))
            if semantic:
                readers.append(reader_stack.enter_context(_open_family_reader(
                    subjects, receipts, policies, generation_id=generation_id, guard=guard)))

        artifact = None
        try:
            artifact = _verify_generation_source(source, footer, expected_pin=expected_pin, admit_receipts=admit,
                                                 semantic=semantic)
            if len(registered) != 1 or len(readers) != int(semantic):
                raise ValueError("Conversion generation requires one complete receipt admission")
            guard()
            # Consume the platform's actual byte-pass receipts, not just a saved
            # success flag or a freshly observed file signature.
            if artifact.local_member_states is None or any(
                    LocalFileState.from_stat((directory / key).lstat()) != state
                    for key, state in artifact.local_member_states.items()):
                raise ValueError("Conversion generation differs from its verified local member states")
        except BaseException:
            try:
                reader_stack.close()
            finally:
                if artifact is not None and artifact.local_member_states is not None:
                    artifact.local_member_states.close()
            raise
        self._stack.callback(artifact.local_member_states.close)
        reader = readers[0] if semantic else None
        if reader is not None:
            reader.family = artifact.root['spec']['family']
        held = (artifact, reader, source)
        self._generations[directory] = held
        self._policies[directory] = registered[0]
        self._guards[directory] = guard
        self._reader_stacks[directory] = reader_stack
        return held

    def verified_source(self, directory):
        """Return an actual byte-admitted source only while its original state remains current."""
        directory = Path(directory).absolute()
        if not self.active or directory not in self._generations:
            raise ValueError("Generation has no admission in this live conversion operation")
        self._guards[directory]()
        artifact, _, source = self._generations[directory]
        return artifact, source

    def policies(self, directory):
        """The receipt policies this operation admitted for ``directory``, by dataset, whether or not a reader opened."""
        self.verified_source(directory)
        return self._policies[Path(directory).absolute()]

    def release_generation(self, directory):
        """Release bounded replay state before admitting another source; retain guarded byte proof."""
        directory = Path(directory).absolute()
        self.verified_source(directory)
        self._reader_stacks[directory].close()

    def verify_public_generation(self, base, index, family, directory, client, bucket):
        """Hold the published generation to this operation's admitted candidate: no copy, no second admission.

        The public root and every manifest must equal the admitted bytes. Each member is held to its admitted
        descriptor by one HEAD against the hash storage computed (:func:`publication.verify_stored_member`); only a
        member with no comparable server hash streams through a SHA-256 from the public URL. Identical bytes admit
        identically, so admitting a downloaded copy again proved nothing more.
        """
        from rulespec_artifacts import iter_member_descriptors
        from spicy_regs.sources import publication

        artifact, source = self.verified_source(directory)
        members = list(iter_member_descriptors(artifact, source))
        entry = index['families'][family]
        if (family != artifact.root['spec']['family']
                or publication._generation_entry(entry) != publication.generation_entry(artifact, members)):
            raise ValueError("Public generation differs from the admitted conversion candidate")
        for key in ['artifact.json', *(item['objectKey'] for item in artifact.root['memberManifests'])]:
            raw = publication._bounded_get(f"{base.rstrip('/')}/{entry['prefix']}/{key}",
                                          allow_missing=False, limit=publication.EVIDENCE_CONTROL_LIMIT)
            with source.open(key) as local:
                if raw is None or raw != local.read(publication.EVIDENCE_CONTROL_LIMIT + 1):
                    raise ValueError("Public generation root or manifest differs from its qualified bytes")
        verified_by = {'sha256': 0, 'md5': 0, 'streamed': 0}
        streamed = 0
        for member in members:
            pinned = publication.Member(f"{entry['prefix']}/{member.object_key}", member.sha256, member.byte_size,
                                        member.record_count)
            how = publication.verify_stored_member(client, bucket, pinned,
                                                   lambda key=member.object_key: source.open(key))
            if how is None:
                streamed += publication.verify_member(base, pinned)
                how = 'streamed'
            verified_by[how] += 1
        self.verified_source(directory)
        return {'artifactDigest': artifact.pin.artifact_digest, 'members': len(members),
                'bytes': sum(member.byte_size for member in members), 'verified_by': verified_by,
                'streamed_bytes': streamed, 'root_and_manifests': 'exact', 'retained_payload_copies': 0}
