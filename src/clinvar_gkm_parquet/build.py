"""Build: the section pipeline, the table stage, self-checks and the manifest (design plan §6.2, §7).

Owned by the build/verify work package.

`build()` streams the full bundle once through a SectionPipeline (with the deltas replayed in the stream,
replay.py) and builds each section's tables as soon as the section has been read, in a worker thread, while the
reader goes on to the next section. There is no state database: a section's rows live in DuckDB only while its
tables are built. `build_tables()` is the table stage on its own (it needs only a DuckDB connection holding the
`bundle_<section>` tables), so it can be tested without real bundles.

Determinism (.claude/rules/python.md): each file is written by one `COPY (SELECT * FROM (<sql>) ORDER BY
<sort_key>)` with the spec's compression and row-group size and key-value metadata that holds no timestamp.
Measured with DuckDB 1.5.4 (2026-10-20): the bytes are identical across runs, across `threads` 1, 4 and 8, with
`preserve_insertion_order` on or off, and when the sort spills to disk under a small `memory_limit`. That holds
because the sort key is unique (the self-check asserts it): with ties, the order of tied rows could depend on
threads. The build still sets `preserve_insertion_order = true` (DuckDB's default) so `COPY` keeps the
`ORDER BY` order. So the order in which a section's rows are staged (the replayed rows come last) and the
order in which the tables are built don't change the files.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import shutil
import sys
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import __version__, baseline, bundle, replay, sources, sqltemplate
from .compat import BUILTIN_SPECS, schema_sections
from .errors import BundleFormatError, InputError, SchemaVersionError, SpecError, ToolError
from .model import (
    BuildInputs,
    BuildResult,
    DeltaBundle,
    DeltaManifest,
    InputFile,
    SectionChunks,
    StreamedBundle,
    TableResult,
)
from .spec import Spec, SpecSource, Table, load_spec, resolve_spec
from .spec import kv_metadata as spec_kv_metadata

KV_PREFIX = "clinvar_gkm_parquet."
# Design plan §7.2: the keys every file carries.
KV_KEYS = (
    "tool_version",
    "table_spec_version",
    "table",
    "bundle_schema_version",
    "source_bundle_sha256",
    "source_deltas",
    "source_release",
    "duckdb_version",
)
# Every key, in the order it is written: KV_KEYS, plus the spec's name (build() always writes it; a file without
# it predates named specs and was written by clinvar-gkm, spec.spec_from_kv), the two keys of a custom spec
# (spec.kv_metadata) and, in a selective build only, the selection (ADR 0004; select.SELECTION_METADATA_KEY).
KV_ORDER = (
    "tool_version",
    "table_spec",
    "table_spec_version",
    "table_spec_builtin",
    "table_spec_sha256",
    "table",
    "bundle_schema_version",
    "source_bundle_sha256",
    "source_deltas",
    "source_release",
    "duckdb_version",
    "selection",
)
DERIVED_LABEL = "derived from the bundle; may be regenerated"
MANIFEST_NAME = "manifest.json"  # provisional (design plan §7.3)


# --- small helpers ---------------------------------------------------------------------------------------------


def sql_str(value: str) -> str:
    """A SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    """A quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def bundle_table(section: str) -> str:
    return sql_ident(f"bundle_{section}")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def key_column(table: Table) -> str | None:
    """The column that holds the bundle key (derivation kind `key`), if the table has one."""
    for c in table.columns:
        if c.derivation.kind == "key":
            return c.name
    return None


def record_column(table: Table) -> str | None:
    """The column that holds the bundle record byte for byte (derivation kind `record`), if any."""
    for c in table.columns:
        if c.derivation.kind == "record":
            return c.name
    return None


def describe_file(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_str(str(path))})").fetchall()
    return [(r[0], r[1]) for r in rows]


def configure(
    con: duckdb.DuckDBPyConnection,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    temp_directory: Path | None = None,
) -> None:
    """Connection settings for a build or a verify run."""
    con.execute("SET preserve_insertion_order = true")
    if memory_limit:
        con.execute(f"SET memory_limit = {sql_str(memory_limit)}")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if temp_directory:
        temp_directory.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = {sql_str(str(temp_directory))}")


def relation_kind(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    """'table', 'view' or None for a relation in the main schema."""
    if con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ? AND schema_name = 'main'", [name]
    ).fetchone():
        return "table"
    if con.execute(
        "SELECT 1 FROM duckdb_views() WHERE view_name = ? AND schema_name = 'main' AND NOT internal", [name]
    ).fetchone():
        return "view"
    return None


def section_counts(con: duckdb.DuckDBPyConnection, sections: Iterable[str]) -> dict[str, int]:
    out = {}
    for s in sorted(set(sections)):
        if relation_kind(con, f"bundle_{s}") is None:
            raise SpecError(f"section {s}: no bundle_{s} table in the database")
        out[s] = con.execute(f"SELECT count(*) FROM {bundle_table(s)}").fetchone()[0]
    return out


# --- key-value metadata ----------------------------------------------------------------------------------------


def kv_metadata(
    *,
    table_spec_version: str,
    bundle_schema_version: str,
    source_bundle_sha256: str,
    source_deltas: list[dict],
    source_release: str | None,
    table_spec: dict[str, str] | None = None,
    selection: str | None = None,
) -> dict[str, str]:
    """The §7.2 metadata shared by every file of one build (the `table` key is added per file). No timestamps.
    `table_spec` is `spec_kv(...)` (its table_spec_version must agree); `selection` is
    SelectionResult.metadata_json(), for a selective build only."""
    kv = {
        "tool_version": __version__,
        "table_spec_version": table_spec_version,
        "bundle_schema_version": bundle_schema_version,
        "source_bundle_sha256": source_bundle_sha256,
        "source_deltas": json.dumps(
            [{"release": d["release"], "sha256": d["sha256"]} for d in source_deltas], separators=(",", ":")
        ),
        "source_release": source_release or "",
        "duckdb_version": duckdb.__version__,
    }
    if table_spec:
        if table_spec.get("table_spec_version", table_spec_version) != table_spec_version:
            raise SpecError(
                f"table spec entries {table_spec} disagree with table_spec_version {table_spec_version}"
            )
        kv.update(table_spec)
    if selection is not None:
        kv["selection"] = selection
    return kv


def spec_kv(spec: Spec, source: SpecSource | None) -> dict[str, str]:
    """The spec's key-value entries (spec.kv_metadata). A spec given without its source is labelled by its own
    name and version, and marked custom unless it is the built-in of that name and version. An unnamed spec
    (an in-memory parse_spec mapping, for tests; every spec file has a name) writes its version only."""
    if source is not None:
        return spec_kv_metadata(source)
    if spec.name is None:
        return {"table_spec_version": spec.spec_version}
    if BUILTIN_SPECS.get(spec.name) == spec.spec_version and load_spec(spec.spec_version, spec.name) == spec:
        return {"table_spec": spec.name, "table_spec_version": spec.spec_version}
    return {"table_spec": spec.name, "table_spec_version": spec.spec_version, "table_spec_builtin": "false"}


def render_table(table: Table) -> str:
    """The table's SELECT: the baseline renderer for key/record/refs tables, else the table's SQL template
    (baseline.renderer_for)."""
    return baseline.renderer_for(table)(table)


def _kv_sql(kv: dict[str, str], table: str) -> str:
    full = {**kv, "table": table}
    missing = [k for k in KV_KEYS if k not in full]
    if missing:
        raise SpecError(f"key-value metadata is missing {missing}")
    unknown = sorted(set(full) - set(KV_ORDER))
    if unknown:
        raise SpecError(f"unknown key-value metadata {unknown}")
    items = ", ".join(f"{sql_str(KV_PREFIX + k)}: {sql_str(full[k])}" for k in KV_ORDER if k in full)
    return "{" + items + "}"


# --- the table stage -------------------------------------------------------------------------------------------


@dataclass
class _Group:
    """Tables whose rows together are the key set of `sections`: one table without a row filter, or the tables
    that split one section by a row filter (the evidence-line split)."""

    sections: tuple[str, ...]
    tables: list[str]
    complete: bool  # every table of the group is in this build


def table_groups(spec: Spec, names: list[str]) -> dict[str, _Group]:
    """table name -> its group."""
    by_name: dict[str, _Group] = {}
    partitions: dict[tuple[str, ...], list[str]] = {}
    for t in spec.tables:
        if t.row_filter is None:
            by_name[t.name] = _Group(t.sections, [t.name], True)
        else:
            partitions.setdefault(t.sections, []).append(t.name)
    for sections, members in partitions.items():
        g = _Group(sections, [m for m in members if m in names], all(m in names for m in members))
        for m in members:
            by_name[m] = g
    return by_name


def build_order(spec: Spec, names: list[str], counts: dict[str, int]) -> list[Table]:
    """Smallest first, largest last (design plan §6.2), ties by name; the tables of one row-filter group are
    kept together so the group check runs as soon as its last member is written."""
    tables = [spec.table(n) for n in names]

    def size(t: Table) -> int:
        return sum(counts.get(s, 0) for s in t.sections)

    return sorted(tables, key=lambda t: (size(t), t.sections, t.name))


def _fingerprint_file(con, path: Path, kcol: str | None, rcol: str | None) -> tuple[int, int | None]:
    """(rows, order-independent sum of hash(key, record)) of a written file."""
    if kcol is None:
        return con.execute(f"SELECT count(*) FROM read_parquet({sql_str(str(path))})").fetchone()[0], None
    args = sql_ident(kcol) + (f", {sql_ident(rcol)}" if rcol else "")
    n, h = con.execute(
        f"SELECT count(*), coalesce(sum(hash({args})), 0) FROM read_parquet({sql_str(str(path))})"
    ).fetchone()
    return n, int(h)


def _fingerprint_sections(con, sections: tuple[str, ...], with_record: bool) -> tuple[int, int]:
    args = "key, record" if with_record else "key"
    union = " UNION ALL ".join(f"SELECT key, record FROM {bundle_table(s)}" for s in sections)
    n, h = con.execute(f"SELECT count(*), coalesce(sum(hash({args})), 0) FROM ({union})").fetchone()
    return n, int(h)


def _mismatch_samples(con, paths: list[tuple[Path, str, str | None]], sections, limit: int = 5) -> str:
    """On a fingerprint failure, a few keys that differ (for the error message)."""
    union_t = " UNION ALL ".join(
        f"SELECT {sql_ident(k)} AS key, {('md5(' + sql_ident(r) + ')') if r else 'NULL'} AS h "
        f"FROM read_parquet({sql_str(str(p))})"
        for p, k, r in paths
    )
    union_s = " UNION ALL ".join(f"SELECT key, md5(record) AS h FROM {bundle_table(s)}" for s in sections)
    with_record = all(r for _, _, r in paths)
    cond = "t.key IS NULL OR s.key IS NULL" + (" OR t.h IS DISTINCT FROM s.h" if with_record else "")
    rows = con.execute(
        f"""SELECT coalesce(t.key, s.key) AS k,
                   CASE WHEN s.key IS NULL THEN 'not in bundle' WHEN t.key IS NULL THEN 'missing'
                        ELSE 'data differs' END
            FROM ({union_t}) t FULL OUTER JOIN ({union_s}) s ON t.key = s.key
            WHERE {cond} ORDER BY k LIMIT {limit}"""
    ).fetchall()
    return "; ".join(f"{k} ({why})" for k, why in rows) or "no differing key found (hash collision?)"


def _check_subset(con, table: Table, path: Path) -> None:
    """For a row-filter table built without its siblings: every (id, data) is a (key, record) of its section."""
    kcol, rcol = key_column(table), record_column(table)
    if kcol is None:
        return
    h = "md5_number(record)" if rcol else "NULL::HUGEINT"
    union_s = " UNION ALL ".join(f"SELECT key, {h} AS h FROM {bundle_table(s)}" for s in table.sections)
    th = f"md5_number({sql_ident(rcol)})" if rcol else "NULL::HUGEINT"
    rows = con.execute(
        f"""SELECT t.k FROM (SELECT {sql_ident(kcol)} AS k, {th} AS h FROM read_parquet({sql_str(str(path))})) t
            ANTI JOIN ({union_s}) s ON t.k = s.key AND t.h IS NOT DISTINCT FROM s.h ORDER BY 1 LIMIT 5"""
    ).fetchall()
    if rows:
        raise SpecError(
            f"table {table.name}: rows whose (id, data) is not a (key, record) of section(s) "
            f"{', '.join(table.sections)}: {', '.join(r[0] for r in rows)}"
        )


def _check_table(con, table: Table, path: Path) -> None:
    """Per-file checks: DESCRIBE equals the spec; the sort key is unique (needed for byte determinism)."""
    got = describe_file(con, path)
    want = table.describe()
    if got != want:
        pairs = enumerate(zip(got, want, strict=False))
        diffs = [f"{i}: built {g!r} != spec {w!r}" for i, (g, w) in pairs if g != w]
        if len(got) != len(want):
            diffs.append(f"built {len(got)} columns, spec {len(want)}")
        raise SpecError(f"table {table.name}: DESCRIBE differs from the spec: {'; '.join(diffs[:5])}")
    sk = sql_ident(table.sort_key)
    dup = con.execute(
        f"SELECT count(*) - count(DISTINCT {sk}), count_if({sk} IS NULL) FROM read_parquet({sql_str(str(path))})"
    ).fetchone()
    if dup[0] or dup[1]:
        raise SpecError(
            f"table {table.name}: sort key {table.sort_key} is not unique and non-NULL "
            f"({dup[0]} duplicate, {dup[1]} NULL); the output would not be deterministic"
        )


def _check_group(con, spec: Spec, group: _Group, written: dict[str, Path], fps: dict[str, tuple]) -> None:
    """Rows of the group's tables equal the section key count, and the (id, data) pairs of the files equal the
    (key, record) pairs of the section(s). The pairs are compared by an order-independent fingerprint, the
    exact (HUGEINT) sum of DuckDB's 64-bit hash(key, record): one streaming pass per side, no join. A join runs
    only on failure, to name keys. `verify --bundle-recompute` is the per-key check."""
    tables = [spec.table(n) for n in group.tables]
    label = ", ".join(group.tables)
    rows = sum(fps[n][0] for n in group.tables)
    with_key = all(key_column(t) for t in tables)
    with_record = with_key and all(record_column(t) for t in tables)
    n_s, h_s = _fingerprint_sections(con, group.sections, with_record)
    if rows != n_s:
        detail = ", ".join(f"{n}={fps[n][0]}" for n in group.tables)
        raise SpecError(
            f"table(s) {label}: {rows} rows ({detail}) but section(s) {', '.join(group.sections)} hold {n_s} keys"
            + (
                f"; differing keys: {_mismatch_samples(con, _paths(tables, written), group.sections)}"
                if with_key
                else ""
            )
        )
    if not with_key:
        return
    if with_record:
        h_t = sum(fps[n][1] for n in group.tables)
    else:  # some table has no record column: compare keys alone on both sides
        h_t = sum(_fingerprint_file(con, written[t.name], key_column(t), None)[1] for t in tables)
    if h_t != h_s:
        raise SpecError(
            f"table(s) {label}: (id, data) pairs differ from the (key, record) pairs of section(s) "
            f"{', '.join(group.sections)}: {_mismatch_samples(con, _paths(tables, written), group.sections)}"
        )


def _paths(tables: list[Table], written: dict[str, Path]) -> list[tuple[Path, str, str | None]]:
    return [(written[t.name], key_column(t), record_column(t)) for t in tables]


def _copy_sql(spec: Spec, table: Table, sql: str, path: Path, kv: dict[str, str]) -> str:
    compression = str(spec.writer.get("compression", ""))
    if not re.fullmatch(r"[a-z0-9_]+", compression):
        raise SpecError(f"spec writer.compression {compression!r} is not a codec name")
    try:
        row_group_size = int(spec.writer["row_group_size"])
    except (KeyError, TypeError, ValueError) as e:
        raise SpecError("spec writer.row_group_size must be an integer") from e
    body = sql.strip().rstrip(";")
    return (
        f"COPY (SELECT * FROM (\n{body}\n) ORDER BY {sql_ident(table.sort_key)})\n"
        f"TO {sql_str(str(path))} (FORMAT parquet, COMPRESSION {compression}, "
        f"ROW_GROUP_SIZE {row_group_size}, KV_METADATA {_kv_sql(kv, table.name)})"
    )


def build_tables(
    con: duckdb.DuckDBPyConnection,
    spec: Spec,
    table_names: list[str],
    out_dir: Path,
    kv: dict[str, str],
    *,
    render: Callable[[Table], str] | None = None,
    counts: dict[str, int] | None = None,
    on_section_done: Callable[[str], None] | None = None,
    log: Callable[[str], None] | None = None,
) -> list[TableResult]:
    """Write and self-check `<out_dir>/<table>.parquet` for each named table (design plan §6.2 steps 3-4).

    `con` must hold `bundle_<section>` for every section the tables read. Each file is written to a `.tmp` name
    and renamed only after its checks pass (for a row-filter group: after the whole group passes), so a failed
    build never leaves a file that looks valid. `on_section_done(section)` is called once no remaining table
    reads that section. Returns the results in build order.
    """
    render = render or render_table
    unknown = [n for n in table_names if n not in {t.name for t in spec.tables}]
    if unknown:
        raise SpecError(f"unknown table(s) {unknown}; the spec lists {sorted(t.name for t in spec.tables)}")
    names = list(dict.fromkeys(table_names))
    if counts is None:
        counts = section_counts(con, {s for n in names for s in spec.table(n).sections})
    out_dir.mkdir(parents=True, exist_ok=True)
    order = build_order(spec, names, counts)
    last_use = {s: i for i, t in enumerate(order) for s in t.sections}
    groups = table_groups(spec, names)

    written: dict[str, Path] = {}
    fps: dict[str, tuple] = {}
    pending: dict[int, list[str]] = {}  # id(group) -> tables waiting for the group check
    results: dict[str, TableResult] = {}
    try:
        for i, table in enumerate(order):
            if log:
                log(f"table {table.name}: writing")
            tmp = out_dir / (table.file_name + ".tmp")
            tmp.unlink(missing_ok=True)
            written[table.name] = tmp  # registered first, so a failed COPY's partial file is removed too
            try:
                con.execute(_copy_sql(spec, table, render(table), tmp, kv))
            except duckdb.Error as e:
                raise SpecError(f"table {table.name}: query failed: {e}") from e
            _check_table(con, table, tmp)
            fps[table.name] = _fingerprint_file(con, tmp, key_column(table), record_column(table))
            group = groups[table.name]
            if not group.complete:
                _check_subset(con, table, tmp)
                ready = [table.name]
            else:
                pending.setdefault(id(group), []).append(table.name)
                if len(pending[id(group)]) < len(group.tables):
                    ready = []
                else:
                    _check_group(con, spec, group, written, fps)
                    ready = pending.pop(id(group))
            for name in ready:
                final = out_dir / spec.table(name).file_name
                os.replace(written[name], final)
                written[name] = final
                results[name] = TableResult(
                    table=name, file=final, rows=fps[name][0], sha256=file_sha256(final), size=final.stat().st_size
                )
                if log:
                    log(f"table {name}: {fps[name][0]} rows")
            if on_section_done:
                for s in table.sections:
                    if last_use[s] == i:
                        on_section_done(s)
    finally:
        for p in written.values():
            if p.suffix == ".tmp":
                p.unlink(missing_ok=True)
    return [results[t.name] for t in order]


# --- the section pipeline --------------------------------------------------------------------------------------

CHUNK_BYTES = 8 << 20  # uncompressed bytes per staging chunk file; DuckDB reads a section's chunks in parallel
QUEUE_SIZE = 1  # closed sections that may wait for the worker while the reader stages the next one
# read_csv's buffer per chunk file being read: DuckDB's default, 16 x max_line_size, is 32 MiB a file at the
# default max_line_size, per thread. The longest line is known (the writer measures it), so size from that.
_MIN_CSV_BUFFER = 1 << 20
_POLL_S = 0.1


class _Stopped(ToolError):
    """The reader stops because the worker failed (the worker's error is the one raised)."""


class _Counter:
    """Stands in for a SectionWriter for a section that is read and checked but not staged."""

    paths: tuple = ()
    max_line = 0

    def __init__(self):
        self.records = 0

    def add(self, key: bytes, record: bytes) -> None:
        self.records += 1

    def close(self) -> None:
        pass

    def abort(self) -> None:
        pass


@dataclass
class StreamState:
    """What a pipeline run read: the full, the deltas, and every section's final record count."""

    full: StreamedBundle
    deltas: list[tuple[DeltaBundle, DeltaManifest]]
    counts: dict[str, int]


def _read_chunks(item: SectionChunks) -> str:
    """SQL reading a section's chunk files as (key, record): the key decoded from its JSON form, the record as
    is."""
    files = "[" + ", ".join(sql_str(str(p)) for p in item.paths) + "]"
    max_line = item.max_line_bytes + 1024
    buffer = max(_MIN_CSV_BUFFER, 2 * max_line)
    return (
        f"SELECT json_extract_string(k, '$') AS key, record FROM read_csv({files}, delim='\t', quote='', "
        f"escape='', header=false, auto_detect=false, new_line='\\n', strict_mode=true, compression='gzip', "
        f"columns={{'k': 'VARCHAR', 'record': 'VARCHAR'}}, max_line_size={max_line}, buffer_size={buffer})"
    )


def load_section(
    con: duckdb.DuckDBPyConnection, item: SectionChunks, source_name: str, *, materialize: bool = False
) -> int:
    """Make `bundle_<section>(key, record)` over a section's chunk files and check, in one scan, the row count
    and that no key repeats (the duplicate check the reader leaves to DuckDB, bundle.py). Returns the row count.

    By default `bundle_<section>` is a VIEW, so the rows are read from the chunks by each query and never copied
    into DuckDB: measured, a copy costs more disk (a DuckDB file is larger than the JSON it holds) or memory
    (an in-memory table holds the JSON uncompressed) and is no faster. The chunks must then stay until the
    section is done. `materialize` makes a TABLE instead (load_inputs), after which the chunks can go."""
    t = bundle_table(item.section)
    if not item.paths:
        con.execute(f"CREATE TABLE {t} (key VARCHAR, record VARCHAR)")
        return 0
    con.execute(f"CREATE {'TABLE' if materialize else 'VIEW'} {t} AS {_read_chunks(item)}")
    n, distinct = con.execute(f"SELECT count(*), count(DISTINCT key) FROM {t}").fetchone()
    if n != item.records:
        raise InputError(f"{source_name}: section {item.section}: staged {item.records} records, loaded {n}")
    if distinct != n:
        k, c = con.execute(
            f"SELECT key, count(*) FROM {t} GROUP BY key HAVING count(*) > 1 ORDER BY key LIMIT 1"
        ).fetchone()
        raise BundleFormatError(f"{source_name}: section {item.section}, key {k}: appears {c} times")
    return n


class SectionPipeline:
    """One streaming pass of the full (design plan §6.2), shared by build, `verify --bundle-recompute` and
    load_inputs.

    The reader (the calling thread) checks the chain, reads the deltas into memory and folds them (replay.py),
    then streams the full once. It stages only the current section, as chunk files holding its final state, and
    when the section closes puts it on a bounded queue. A worker thread takes each section, makes
    `bundle_<section>` over its chunks in a DuckDB connection (load_section), calls `process(con, section,
    rows)`, then drops it and deletes the chunks, while the reader goes on to the next section. Every bundle
    section is handed over once, in bundle order; a section absent from the full (empty, or only in deltas)
    at its place in that order.

    Bounds: the chunks of at most QUEUE_SIZE + 2 sections exist at once (the one the worker has, those queued,
    the one being read), and one DuckDB connection runs at a time, so disk and memory follow the largest
    sections, not the bundle. A failure on either side sets `stop`: the reader stops at its next buffered write
    (about every MiB) or hand-over, the worker's running query is interrupted, and the first error is raised
    once the worker has exited. Only sections in `needed` are staged; the others are still read, checked and
    counted. With `con`, every section is loaded as a table into that one connection and kept (load_inputs)."""

    def __init__(
        self,
        inputs: BuildInputs,
        spec: Spec,
        work: Path,
        *,
        process: Callable[[duckdb.DuckDBPyConnection, str, int], None],
        needed: set[str],
        on_begin: Callable[[SectionPipeline, str], None] | None = None,
        con: duckdb.DuckDBPyConnection | None = None,
        memory_limit: str | None = None,
        threads: int | None = None,
        keep_work: bool = False,
        log: Callable[[str], None] | None = None,
        queue_size: int | None = None,
        chunk_bytes: int | None = None,
        chain: replay.Chain | None = None,
    ):
        self.inputs, self.spec, self.work = inputs, spec, Path(work)
        self.chain = chain  # the deltas already read (replay.load_chain), or None to read them here
        self.schema_version: str | None = None
        self.process, self.needed, self.on_begin = process, needed, on_begin
        self.shared_con = con
        self.memory_limit, self.threads, self.keep_work = memory_limit, threads, keep_work
        self.log = log or (lambda m: None)
        self.chunk_bytes = chunk_bytes or CHUNK_BYTES
        self.manifests: list[DeltaManifest] = []
        self.deltas: list[tuple[DeltaBundle, DeltaManifest]] = []
        self.counts: dict[str, int] = {}
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self._active: duckdb.DuckDBPyConnection | None = None
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size or QUEUE_SIZE)
        self._changes: dict[str, replay.SectionChanges] = {}
        self._releases: list[str] = []
        self._order = {s: i for i, s in enumerate(bundle.SECTIONS)}
        self._last = -1  # position of the last section opened in the full
        self._writer = None
        self._replay: replay.SectionReplay | None = None
        self._stage = self.work / "stage"

    # --- run ---------------------------------------------------------------------------------------------------

    def run(self) -> StreamState:
        self._stage.mkdir(parents=True, exist_ok=True)
        inputs = self.inputs
        # The chain is checked from the manifests before any bundle is read (replay.load_chain).
        chain = self.chain if self.chain is not None else replay.load_chain(inputs, self.log)
        self.manifests, self.deltas, self._changes = chain.manifests, chain.deltas, chain.changes
        self._releases = chain.releases

        worker = threading.Thread(target=self._work, name="clinvar-gkm-parquet-worker")
        worker.start()
        try:
            self.log(
                f"streaming {inputs.full.name}" + (f" with {len(self.deltas)} delta(s)" if self.deltas else "")
            )
            full = bundle.stream_bundle(inputs.full, self)
            for s in bundle.SECTIONS:  # sections after the full's last one
                if s not in self.counts:
                    self._absent_section(s)
            self._put(None)
        except BaseException as e:
            self.stop.set()
            con = self._active
            if con is not None:
                try:
                    con.interrupt()
                except Exception:  # noqa: BLE001 (best effort: the worker may have just closed it)
                    pass
            worker.join()
            if self._writer is not None:
                self._writer.abort()
            if isinstance(e, _Stopped) and self.error is not None:
                raise self.error from None
            raise
        worker.join()
        if self.error is not None:
            raise self.error
        return StreamState(full=full, deltas=self.deltas, counts=dict(self.counts))

    def _work(self) -> None:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=_POLL_S)
                except queue.Empty:
                    if self.stop.is_set():
                        return
                    continue
                if item is None or self.stop.is_set():
                    return
                self._handle(item)
        except BaseException as e:  # noqa: BLE001 (handed to the reader, which raises it)
            self.error = e
            self.stop.set()

    def _handle(self, item: SectionChunks) -> None:
        shared = self.shared_con is not None
        if shared:
            con = self.shared_con
        else:
            con = duckdb.connect()
            configure(
                con, memory_limit=self.memory_limit, threads=self.threads, temp_directory=self.work / "duckdb-tmp"
            )
        self._active = con
        try:
            rows = load_section(con, item, self.inputs.full.name, materialize=shared)
            if not shared:
                self.process(con, item.section, rows)
        finally:
            self._active = None
            if not shared:
                con.close()
            if not self.keep_work:
                for p in item.paths:
                    p.unlink(missing_ok=True)

    def _check_stop(self) -> None:
        if self.stop.is_set():
            raise _Stopped("stopped: the build worker failed")

    def _put(self, item: SectionChunks | None) -> None:
        while True:
            self._check_stop()
            try:
                self._queue.put(item, timeout=_POLL_S)
                return
            except queue.Full:
                continue

    # --- the sink (bundle.stream_bundle calls these in the reader thread) ---------------------------------------

    def begin(self, schema_version: str) -> None:
        spec = self.spec
        if schema_version not in spec.bundle_schema_versions:
            raise SchemaVersionError(
                f"bundle schema_version {schema_version} is not one table spec {spec.spec_version} reads "
                f"({', '.join(spec.bundle_schema_versions)})"
            )
        replay.check_schema_versions(self.inputs.full.name, schema_version, self.deltas)
        self.schema_version = schema_version
        if self.on_begin is not None:
            self.on_begin(self, schema_version)

    def open_section(self, section: str):
        pos = self._order[section]
        if pos < self._last:
            raise BundleFormatError(
                f"{self.inputs.full.name}: section {section} comes after {bundle.SECTIONS[self._last]}; the "
                f"assembler writes the sections in the bundle schema's order ({', '.join(bundle.SECTIONS)})"
            )
        self._last = pos
        for s in bundle.SECTIONS[:pos]:  # sections the full lacks, at their place in the order
            if s not in self.counts:
                self._absent_section(s)
        return self._open(section)

    def close_section(self, section: str) -> None:
        self._close(section)

    # --- sections ----------------------------------------------------------------------------------------------

    def _open(self, section: str):
        if section in self.needed and section in self.spec.sections:
            d = self._stage / section
            d.mkdir()
            w = bundle.SectionWriter(d / section, chunk_bytes=self.chunk_bytes, check=self._check_stop)
        else:
            w = _Counter()
        self._writer = w
        changes = self._changes.get(section)
        self._replay = replay.SectionReplay(changes, self.inputs.full.name, self._releases) if changes else None
        return self._replay.wrap(w.add) if self._replay else w.add

    def _close(self, section: str) -> None:
        w = self._writer
        if self._replay is not None:
            self._replay.finish(w.add)
        w.close()
        self._writer = self._replay = None
        self.counts[section] = w.records
        # A spec may read a subset of the sections (a custom spec); a section the bundle schema doesn't have is
        # never dropped silently (bundle.py refuses it while reading; this is the guard for the final state).
        if w.records and section not in schema_sections(self.schema_version):
            raise SpecError(
                f"section {section} is not a section of bundle schema {self.schema_version}; refusing to drop it"
            )
        if isinstance(w, bundle.SectionWriter):
            self._put(SectionChunks(section, tuple(w.paths), w.records, w.max_line))

    def _absent_section(self, section: str) -> None:
        """A section the full lacks: its final state is the deltas' upserts, if any."""
        self._open(section)
        self._close(section)


# --- the whole build -------------------------------------------------------------------------------------------


def default_work_dir(out_dir: Path) -> Path:
    out = out_dir.resolve()
    return out.parent / f"{out.name}.work"


def _input_record(inp: InputFile, streamed: StreamedBundle | DeltaBundle | None = None) -> dict:
    rec = {
        "source": inp.source,
        "name": inp.name,
        "release": inp.release,
        "expected_sha256": inp.expected_sha256,
        "sha256_source": inp.sha256_source,
    }
    if streamed is not None:
        rec["sha256"] = streamed.sha256
        rec["size"] = streamed.size
        rec["schema_version"] = streamed.schema_version
    elif inp.size is not None:
        rec["size"] = inp.size
    return rec


def source_release_of(inputs: BuildInputs, manifests: list[DeltaManifest]) -> str | None:
    """§7.2: the last delta's release; with no deltas, the full's source release (or its own release)."""
    if manifests:
        return manifests[-1].release
    return inputs.full_source_release or inputs.full.release


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_manifest(
    path: Path,
    *,
    kv: dict[str, str],
    spec: Spec,
    inputs: BuildInputs,
    full: StreamedBundle,
    deltas: list[tuple[DeltaBundle, DeltaManifest]],
    counts: dict[str, int],
    results: list[TableResult],
    started_at: str,
    ended_at: str,
    spec_sha256: str | None = None,
) -> dict:
    """The dataset manifest (design plan §7.3). It carries timestamps, so unlike the Parquet files it is not
    byte-reproducible. `table_spec` names the spec as the files' key-value metadata does (plus the sha256 of its
    tables.yaml when known); `selection`, present only for a selective build, is the files' selection entry as
    an object. `inputs` and the source keys name the published bundles; `sections` counts the state the tables
    were built from (for a selective build, the mini-bundle's)."""
    source_release = kv["source_release"] or None
    spec_name = kv.get("table_spec") or spec.name
    subset = " (a selection; see selection)" if "selection" in kv else ""
    manifest = {
        "label": DERIVED_LABEL,
        "derived": True,
        "note": (
            "Parquet tables derived from the ClinVar-GKM JSON bundle(s) listed under inputs by "
            f"clinvar-gkm-parquet {kv['tool_version']} (table spec {spec_name} {kv['table_spec_version']})"
            f"{subset}. The bundles are the record; these files may be regenerated."
        ),
        "build_date": ended_at[:10],
        "source_release": source_release,
        "release": source_release,
        # ClinVar-GKM's line index reads builder, dataset and files[] from this manifest (r2_layout.build_index
        # @ origin/1.0 25344ba; the shape of its own `parquet-manifest` command). dataset names a BigQuery
        # dataset, which the tool can't know.
        "builder": f"clinvar-gkm-parquet {kv['tool_version']}",
        "dataset": None,
        "files": [
            {"name": r.file.name, "size": r.size, "sha256": r.sha256}
            for r in sorted(results, key=lambda r: r.file.name)
        ],
        "tool_version": kv["tool_version"],
        "table_spec": {
            "name": spec_name,
            "version": kv["table_spec_version"],
            "builtin": kv.get("table_spec_builtin") != "false",
            "sha256": kv.get("table_spec_sha256") or spec_sha256,
        },
        "table_spec_version": kv["table_spec_version"],
        "table_spec_status": spec.status,
        "bundle_schema_version": kv["bundle_schema_version"],
        "schema_version": kv["bundle_schema_version"],
        "duckdb_version": kv["duckdb_version"],
        "source_bundle_sha256": kv["source_bundle_sha256"],
        "source_deltas": json.loads(kv["source_deltas"]),
        "inputs": {
            "full": {**_input_record(inputs.full, full), "source_release": inputs.full_source_release},
            "deltas": [
                {
                    "bundle": _input_record(d_inp, delta),
                    "manifest": _input_record(m_inp),
                    "release": man.release,
                    "baseline_release": man.baseline_release,
                }
                for (d_inp, m_inp), (delta, man) in zip(inputs.deltas, deltas, strict=True)
            ],
        },
        "sections": dict(sorted(counts.items())),
        "tables": {
            r.table: {"file": r.file.name, "rows": r.rows, "sha256": r.sha256, "size": r.size}
            for r in sorted(results, key=lambda r: r.table)
        },
        "started_at": started_at,
        "ended_at": ended_at,
        "generated_at": ended_at,
    }
    if "selection" in kv:
        manifest["selection"] = json.loads(kv["selection"])
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def tables_by_section(spec: Spec, names: list[str]) -> dict[str, list[str]]:
    """section -> the named tables that read it. The pipeline builds each table from one section; every table
    of spec 1.0 reads exactly one."""
    out: dict[str, list[str]] = {}
    for n in names:
        t = spec.table(n)
        if len(t.sections) != 1:
            raise SpecError(f"table {n} reads sections {list(t.sections)}; the build reads one section per table")
        out.setdefault(t.sections[0], []).append(n)
    return out


def _prehash(inp: InputFile, log: Callable[[str], None] | None) -> str:
    """The sha256 of a full that has no published checksum. The tables' key-value metadata names it and is
    written before the stream ends, so it is computed first, in a separate pass over the compressed bytes."""
    if log:
        log(f"hashing {inp.name} first (no published sha256)")
    h = hashlib.sha256()
    with sources.open_stream(inp) as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class LoadedState:
    """What `load_inputs` leaves behind: the inputs read and a DuckDB connection holding the final state."""

    con: duckdb.DuckDBPyConnection
    full: StreamedBundle
    deltas: list[tuple[DeltaBundle, DeltaManifest]]
    counts: dict[str, int]


def load_inputs(
    inputs: BuildInputs,
    work: Path,
    spec: Spec,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    log: Callable[[str], None] | None = None,
) -> LoadedState:
    """Every section's final state in ONE in-memory DuckDB connection, as `bundle_<section>` tables (all 20;
    a section absent from every input is an empty table). For tests and small bundles: build and
    `verify --bundle-recompute` stream one section at a time instead."""
    work = Path(work)
    con = duckdb.connect()
    configure(con, memory_limit=memory_limit, threads=threads, temp_directory=work / "duckdb-tmp")
    pipe = SectionPipeline(
        inputs, spec, work, process=lambda *a: None, needed=set(bundle.SECTIONS), con=con, log=log
    )
    try:
        state = pipe.run()
    except BaseException:
        con.close()
        raise
    finally:
        shutil.rmtree(pipe.work / "stage", ignore_errors=True)
    return LoadedState(con, state.full, state.deltas, state.counts)


def build(
    inputs: BuildInputs,
    out_dir: Path,
    *,
    tables: list[str] | None = None,
    work_dir: Path | None = None,
    keep_work: bool = False,
    memory_limit: str | None = None,
    threads: int | None = None,
    spec: Spec | None = None,
    spec_source: SpecSource | None = None,
    driver=None,
    render: Callable[[Table], str] | None = None,
    log: Callable[[str], None] | None = None,
) -> BuildResult:
    """Stream the inputs once and build every table section by section (design plan §6.2), then write the
    manifest.

    `spec` defaults to the default spec, baseline, as the CLI's does (ADR 0003); a build meant to match the
    published set passes the clinvar-gkm spec. `spec_source` (spec.resolve_spec) labels the files, see
    `spec_kv`. With `driver` (a driver.Driver), the selection runs first, writing a mini-bundle in the work
    directory, and the tables are built from it (ADR 0004): every file and the manifest then carry the
    selection, while the source keys and `inputs` still name the published bundles. Without one, the selection
    code is never reached. `render` defaults to `render_table`."""
    if spec is None:
        spec, spec_source = resolve_spec(None)
    render = render or render_table
    names = list(dict.fromkeys(tables)) if tables else [t.name for t in spec.tables]
    unknown = [n for n in names if n not in {t.name for t in spec.tables}]
    if unknown:
        raise SpecError(f"unknown table(s) {unknown}; the spec lists {sorted(t.name for t in spec.tables)}")
    by_section = tables_by_section(spec, names)
    tspec = spec_kv(spec, spec_source)
    if driver is not None:
        from . import driver as driver_mod  # only a selective build needs it

        driver_mod.driver_table(spec, driver)  # checked before anything is read
        driver_mod.check_tables(spec, names, driver, explicit=bool(tables))
    started_at = _now()
    out_dir = Path(out_dir)
    # One directory holds one set: building into a directory that already has table files or a manifest could
    # leave tables from two releases side by side (after a failure, or with --tables).
    if out_dir.is_dir():
        existing = sorted(
            p.name
            for p in out_dir.iterdir()
            if p.name == MANIFEST_NAME or p.name.endswith(".parquet") or p.name.endswith(".parquet.tmp")
        )
        if existing:
            raise InputError(
                f"output directory {out_dir} already holds {', '.join(existing[:5])}"
                f"{' …' if len(existing) > 5 else ''}; build into an empty or new directory"
            )
    out_dir.mkdir(parents=True, exist_ok=True)
    work_root = Path(work_dir) if work_dir else default_work_dir(out_dir)
    created_root = not work_root.exists()
    work = work_root / f"build-{os.getpid()}"
    if work.exists():
        shutil.rmtree(work)
    kv: dict[str, str] = {}
    results: dict[str, TableResult] = {}
    try:
        selected = None
        if driver is not None:
            selected = driver_mod.run_selection(
                inputs,
                spec,
                driver,
                work / "selection" / driver_mod.SELECTION_BUNDLE_NAME,
                work / "selection",
                memory_limit=memory_limit,
                threads=threads,
                log=log,
            )
            read = BuildInputs(full=selected.bundle, full_source_release=None)
            full_sha = selected.full.sha256
            read_sha = selected.result.sha256
        else:
            read = inputs
            full_sha = read_sha = (inputs.full.expected_sha256 or _prehash(inputs.full, log)).lower()

        def on_begin(pipe: SectionPipeline, schema_version: str) -> None:
            chain = selected.chain if selected else pipe
            kv.update(
                kv_metadata(
                    table_spec_version=spec.spec_version,
                    bundle_schema_version=schema_version,
                    source_bundle_sha256=full_sha,
                    source_deltas=[{"release": m.release, "sha256": d.sha256} for d, m in chain.deltas],
                    source_release=source_release_of(inputs, chain.manifests),
                    table_spec=tspec,
                    selection=selected.result.metadata_json() if selected else None,
                )
            )

        def process(con: duckdb.DuckDBPyConnection, section: str, rows: int) -> None:
            con.execute(sqltemplate.macros_sql())
            done = build_tables(
                con, spec, by_section[section], out_dir, kv, render=render, counts={section: rows}, log=log
            )
            results.update((r.table, r) for r in done)

        pipe = SectionPipeline(
            read,
            spec,
            work,
            process=process,
            needed=set(by_section),
            on_begin=on_begin,
            memory_limit=memory_limit,
            threads=threads,
            keep_work=keep_work,
            log=log,
        )
        state = pipe.run()
        if state.full.sha256 != read_sha:
            raise InputError(
                f"{read.full.name}: sha256 {state.full.sha256} differs from {read_sha}, computed before the "
                "build; the file changed while it was read"
            )
        if selected is not None and keep_work and log:
            log(f"kept the mini-bundle: {selected.bundle.source}")
        ordered = [results[n] for n in names]
        manifest_path = out_dir / MANIFEST_NAME
        write_manifest(
            manifest_path,
            kv=kv,
            spec=spec,
            inputs=inputs,
            full=selected.full if selected else state.full,
            deltas=selected.chain.deltas if selected else state.deltas,
            counts=state.counts,
            results=ordered,
            started_at=started_at,
            ended_at=_now(),
            spec_sha256=spec_source.sha256 if spec_source else None,
        )
        return BuildResult(
            out_dir=out_dir,
            source_release=kv["source_release"] or None,
            tables=tuple(ordered),
            manifest_path=manifest_path,
        )
    except BaseException:
        # A failed build leaves no partial set: remove the tables this run wrote (the directory held none).
        for t in spec.tables:
            (out_dir / t.file_name).unlink(missing_ok=True)
            (out_dir / (t.file_name + ".tmp")).unlink(missing_ok=True)
        (out_dir / MANIFEST_NAME).unlink(missing_ok=True)
        raise
    finally:
        if not keep_work:
            shutil.rmtree(work, ignore_errors=True)
            if created_root:
                try:
                    work_root.rmdir()
                except OSError:
                    pass


# --- CLI -------------------------------------------------------------------------------------------------------


def manifest_source_for(delta_source: str) -> str:
    """The delta manifest next to a delta bundle: clinvar-gkm-delta_YYYY-MMDD.manifest.json (provisional name,
    design plan §10) beside clinvar-gkm-delta_YYYY-MMDD.json.gz."""
    for suffix in (".json.gz", ".json"):
        if delta_source.endswith(suffix):
            return delta_source[: -len(suffix)] + ".manifest.json"
    raise InputError(f"can't derive the manifest name from {delta_source!r}; pass --manifest")


def resolve_build_inputs(args) -> BuildInputs:
    """CLI arguments -> BuildInputs (shared by build and verify --bundle-recompute)."""
    release = getattr(args, "release", None)
    index_src = getattr(args, "index", None)
    deltas_src = list(getattr(args, "delta", None) or [])
    manifests_src = list(getattr(args, "manifest", None) or [])
    if release:
        if (
            deltas_src
            or manifests_src
            or getattr(args, "full_source_release", None)
            or getattr(args, "sha256", None)
        ):
            raise InputError(
                "--release resolves the full and deltas (and their checksums) itself; don't combine it with "
                "--delta, --manifest, --full-source-release or --sha256"
            )
        index = sources.load_index(index_src or sources.DEFAULT_INDEX_URL)
        # Index paths start with the line (v1/1.1/...) and are relative to the index's base_url.
        base_url = index.get("base_url") or sources.DEFAULT_BASE_URL
        return sources.resolve_release(index, release, base_url=base_url)
    if not getattr(args, "full", None):
        raise InputError("--full (or --release) is required")
    if manifests_src and len(manifests_src) != len(deltas_src):
        raise InputError(f"{len(manifests_src)} --manifest for {len(deltas_src)} --delta; give one per delta")
    index = sources.load_index(index_src) if index_src else None
    full = sources.resolve_input(args.full, sha256=getattr(args, "sha256", None), index=index)
    manifests_src = manifests_src or [manifest_source_for(d) for d in deltas_src]
    deltas = tuple(
        (sources.resolve_input(d, index=index), sources.resolve_input(m, index=index))
        for d, m in zip(deltas_src, manifests_src, strict=True)
    )
    return BuildInputs(
        full=full,
        full_source_release=getattr(args, "full_source_release", None) or full.release,
        deltas=deltas,
    )


def _log(msg: str) -> None:
    print(f"clinvar-gkm-parquet: {msg}", file=sys.stderr, flush=True)


def run_build_command(args) -> int:
    """`clinvar-gkm-parquet build`: resolve the spec, the selection options and the inputs from the CLI
    arguments, build, print a summary. No --spec means the default spec (baseline)."""
    from .driver import driver_from_args

    spec, source = resolve_spec(getattr(args, "spec", None))
    driver = driver_from_args(args)
    inputs = resolve_build_inputs(args)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None
    result = build(
        inputs,
        args.out,
        tables=tables,
        work_dir=args.work_dir,
        keep_work=args.keep_work,
        memory_limit=args.memory_limit,
        threads=args.threads,
        spec=spec,
        spec_source=source,
        driver=driver,
        log=_log,
    )
    total = sum(t.rows for t in result.tables)
    subset = f", a selection driven by {driver.table}" if driver else ""
    print(
        f"built {len(result.tables)} table(s) of spec {source.label}, {total} rows, source release "
        f"{result.source_release or '?'}{subset}"
    )
    for t in result.tables:
        print(f"  {t.table:<36} {t.rows:>12}  {t.sha256[:12]}")
    print(f"manifest: {result.manifest_path}")
    return 0
