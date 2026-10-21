"""Build stages: stage, replay, tables, self-check, manifest (design plan §6.2, §7).

Owned by the build/verify work package.

`build()` runs the whole pipeline; `build_tables()` is the table stage on its own (it needs only a DuckDB
connection holding the `bundle_<section>` tables), so it can be tested without real bundles.

Determinism (.claude/rules/python.md): each file is written by one `COPY (SELECT * FROM (<sql>) ORDER BY
<sort_key>)` with the spec's compression and row-group size and key-value metadata that holds no timestamp.
Measured with DuckDB 1.5.4 (2026-10-20): the bytes are identical across runs, across `threads` 1, 4 and 8, with
`preserve_insertion_order` on or off, and when the sort spills to disk under a small `memory_limit`. That holds
because the sort key is unique (the self-check asserts it): with ties, the order of tied rows could depend on
threads. The build still sets `preserve_insertion_order = true` (DuckDB's default) so `COPY` keeps the
`ORDER BY` order.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import __version__, bundle, replay, sources, sqltemplate
from .compat import TABLE_SPEC_VERSION
from .errors import InputError, SchemaVersionError, SpecError
from .model import BuildInputs, BuildResult, DeltaManifest, InputFile, StagedBundle, TableResult
from .spec import Spec, Table, load_spec

KV_PREFIX = "clinvar_gkm_parquet."
# Design plan §7.2, in the order they are written.
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


def drop_relation(con: duckdb.DuckDBPyConnection, name: str) -> None:
    kind = relation_kind(con, name)
    if kind:
        con.execute(f"DROP {kind.upper()} {sql_ident(name)}")


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
) -> dict[str, str]:
    """The §7.2 metadata shared by every file of one build (the `table` key is added per file). No timestamps."""
    return {
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


def _kv_sql(kv: dict[str, str], table: str) -> str:
    full = {**kv, "table": table}
    missing = [k for k in KV_KEYS if k not in full]
    if missing:
        raise SpecError(f"key-value metadata is missing {missing}")
    items = ", ".join(f"{sql_str(KV_PREFIX + k)}: {sql_str(full[k])}" for k in KV_KEYS)
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
    render = render or sqltemplate.render
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
            try:
                con.execute(_copy_sql(spec, table, render(table), tmp, kv))
            except duckdb.Error as e:
                raise SpecError(f"table {table.name}: query failed: {e}") from e
            written[table.name] = tmp
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


# --- the whole build -------------------------------------------------------------------------------------------


def default_work_dir(out_dir: Path) -> Path:
    out = out_dir.resolve()
    return out.parent / f"{out.name}.work"


def _input_record(inp: InputFile, staged: StagedBundle | None = None) -> dict:
    rec = {
        "source": inp.source,
        "name": inp.name,
        "release": inp.release,
        "expected_sha256": inp.expected_sha256,
        "sha256_source": inp.sha256_source,
    }
    if staged is not None:
        rec["sha256"] = staged.sha256
        rec["size"] = staged.size
        rec["schema_version"] = staged.schema_version
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
    full: StagedBundle,
    deltas: list[tuple[StagedBundle, DeltaManifest]],
    counts: dict[str, int],
    results: list[TableResult],
    started_at: str,
    ended_at: str,
) -> dict:
    """The dataset manifest (design plan §7.3). It carries timestamps, so unlike the Parquet files it is not
    byte-reproducible."""
    source_release = kv["source_release"] or None
    manifest = {
        "label": DERIVED_LABEL,
        "derived": True,
        "note": (
            "Parquet tables derived from the ClinVar-GKM JSON bundle(s) listed under inputs by "
            f"clinvar-gkm-parquet {kv['tool_version']} (table spec {kv['table_spec_version']}). "
            "The bundles are the record; these files may be regenerated."
        ),
        "build_date": ended_at[:10],
        "source_release": source_release,
        "release": source_release,
        "tool_version": kv["tool_version"],
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
                    "bundle": _input_record(d_inp, staged),
                    "manifest": _input_record(m_inp),
                    "release": man.release,
                    "baseline_release": man.baseline_release,
                }
                for (d_inp, m_inp), (staged, man) in zip(inputs.deltas, deltas, strict=True)
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
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


@dataclass
class LoadedState:
    """What `load_inputs` leaves behind: the staged inputs and the DuckDB connection holding the final state."""

    con: duckdb.DuckDBPyConnection
    full: StagedBundle
    deltas: list[tuple[StagedBundle, DeltaManifest]]
    counts: dict[str, int]


def load_inputs(
    inputs: BuildInputs,
    work: Path,
    spec: Spec,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    log: Callable[[str], None] | None = None,
    keep_staging: bool = False,
) -> LoadedState:
    """Stages 1-2 (design plan §6.2): check the delta chain, stage every input, replay into a DuckDB database
    at `<work>/state.duckdb`. Shared by build and `verify --bundle-recompute`.

    load_state copies the staging files into DuckDB tables (and checks schema_version across the chain), so
    the staging files are read by nothing afterwards: unless `keep_staging`, they are deleted right away, which
    lowers the disk peak (design plan §6.3)."""
    work.mkdir(parents=True, exist_ok=True)
    manifests = [sources.load_manifest(m) for _, m in inputs.deltas]
    replay.check_chain(inputs.full_source_release, manifests)  # cheap; before tens of minutes of staging
    if log:
        log(f"staging {inputs.full.name}")
    full = bundle.stage_bundle(inputs.full, work / "stage" / "full")
    staged_deltas = []
    for i, ((d_inp, _), man) in enumerate(zip(inputs.deltas, manifests, strict=True)):
        if log:
            log(f"staging {d_inp.name}")
        staged_deltas.append((bundle.stage_bundle(d_inp, work / "stage" / f"delta-{i:03d}"), man))
    if full.schema_version not in spec.bundle_schema_versions:
        raise SchemaVersionError(
            f"bundle schema_version {full.schema_version} is not one table spec {spec.spec_version} reads "
            f"({', '.join(spec.bundle_schema_versions)})"
        )
    con = duckdb.connect(str(work / "state.duckdb"))
    configure(con, memory_limit=memory_limit, threads=threads, temp_directory=work / "duckdb-tmp")
    if log:
        log("loading the final state" + (f" ({len(staged_deltas)} delta(s))" if staged_deltas else ""))
    counts = dict(replay.load_state(con, full, staged_deltas))
    if not keep_staging:
        for staged in [full] + [s for s, _ in staged_deltas]:
            for st in staged.sections.values():
                Path(st.path).unlink(missing_ok=True)
    # An empty section (load_state creates all of them) is harmless; a non-empty one no table reads is not.
    unknown = sorted(s for s, n in counts.items() if n and s not in spec.sections)
    if unknown:
        raise SpecError(
            f"bundle section(s) {unknown} are not read by any table of spec {spec.spec_version}; "
            "refusing to drop them"
        )
    for s in sorted(spec.sections - set(counts)):
        # The assembler omits an empty section (design plan §4.2): its tables are empty, not missing.
        if relation_kind(con, f"bundle_{s}") is None:
            con.execute(f"CREATE TABLE {bundle_table(s)} (key VARCHAR, record VARCHAR)")
        counts[s] = 0
    return LoadedState(con, full, staged_deltas, counts)


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
    log: Callable[[str], None] | None = None,
) -> BuildResult:
    """Stage, replay, write every table, self-check, write the manifest (design plan §6.2)."""
    spec = spec or load_spec(TABLE_SPEC_VERSION)
    names = list(tables) if tables else [t.name for t in spec.tables]
    unknown = [n for n in names if n not in {t.name for t in spec.tables}]
    if unknown:
        raise SpecError(f"unknown table(s) {unknown}; the spec lists {sorted(t.name for t in spec.tables)}")
    started_at = _now()
    out_dir = Path(out_dir)
    work_root = Path(work_dir) if work_dir else default_work_dir(out_dir)
    created_root = not work_root.exists()
    work = work_root / f"build-{os.getpid()}"
    if work.exists():
        shutil.rmtree(work)
    state: LoadedState | None = None
    try:
        state = load_inputs(
            inputs, work, spec, memory_limit=memory_limit, threads=threads, log=log, keep_staging=keep_work
        )
        con = state.con
        con.execute(sqltemplate.macros_sql())
        kv = kv_metadata(
            table_spec_version=spec.spec_version,
            bundle_schema_version=state.full.schema_version,
            source_bundle_sha256=state.full.sha256,
            source_deltas=[{"release": m.release, "sha256": s.sha256} for s, m in state.deltas],
            source_release=source_release_of(inputs, [m for _, m in state.deltas]),
        )

        def section_done(section: str) -> None:
            if not keep_work:
                drop_relation(con, f"bundle_{section}")  # frees its space in the work database

        results = build_tables(
            con, spec, names, out_dir, kv, counts=state.counts, on_section_done=section_done, log=log
        )
        manifest_path = out_dir / MANIFEST_NAME
        write_manifest(
            manifest_path,
            kv=kv,
            spec=spec,
            inputs=inputs,
            full=state.full,
            deltas=state.deltas,
            counts=state.counts,
            results=results,
            started_at=started_at,
            ended_at=_now(),
        )
        return BuildResult(
            out_dir=out_dir,
            source_release=kv["source_release"] or None,
            tables=tuple(results),
            manifest_path=manifest_path,
        )
    finally:
        if state is not None:
            state.con.close()
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
        if deltas_src or manifests_src or getattr(args, "full_source_release", None):
            raise InputError(
                "--release resolves the full and deltas itself; don't combine it with --delta, "
                "--manifest or --full-source-release"
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
    """`clinvar-gkm-parquet build`: resolve the inputs from the CLI arguments, build, print a summary."""
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
        log=_log,
    )
    total = sum(t.rows for t in result.tables)
    print(f"built {len(result.tables)} table(s), {total} rows, source release {result.source_release or '?'}")
    for t in result.tables:
        print(f"  {t.table:<36} {t.rows:>12}  {t.sha256[:12]}")
    print(f"manifest: {result.manifest_path}")
    return 0
