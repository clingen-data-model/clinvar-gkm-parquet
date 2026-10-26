"""verify: spec check of a built directory, bundle recompute, and the oracle comparison (design plan §8.2).

Owned by the build/verify work package. Exit codes (CLI): 0 = pass, 1 = differences, 2 = usage or input error.

Every comparison runs in DuckDB over the Parquet files (and, for the recompute, the `bundle_<section>` tables,
one section at a time, streamed by the build's section pipeline), one table at a time, so memory stays bounded
by DuckDB's `memory_limit` (it spills to the work directory).
Python sees only counts, samples and, for the `data` comparison, the rows whose bytes differ, streamed in batches.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import sources
from .build import (
    KV_KEYS,
    KV_ORDER,
    KV_PREFIX,
    MANIFEST_NAME,
    SectionPipeline,
    configure,
    default_work_dir,
    describe_file,
    file_sha256,
    key_column,
    record_column,
    resolve_build_inputs,
    sql_ident,
    sql_str,
    table_groups,
)
from .compat import PUBLISHED_SPEC, TABLE_SPEC_VERSION
from .errors import InputError, SpecError, ToolError
from .model import BuildInputs
from .spec import SPEC_KV_KEYS, Spec, Table, load_spec, resolve_spec, spec_from_kv

SAMPLES = 5
_TRUNCATE = 300
_BATCH = 10_000


def _trunc(v) -> str | None:
    if v is None:
        return None
    s = v if isinstance(v, str) else str(v)
    return s if len(s) <= _TRUNCATE else s[:_TRUNCATE] + "..."


def _pq(path: Path) -> str:
    return f"read_parquet({sql_str(str(path))})"


def read_kv(con: duckdb.DuckDBPyConnection, path: Path) -> dict[str, str]:
    """The clinvar_gkm_parquet.* key-value metadata of one file, prefix removed."""
    rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata({sql_str(str(path))})").fetchall()
    out = {}
    for k, v in rows:
        k = k.decode() if isinstance(k, bytes) else k
        v = v.decode() if isinstance(v, bytes) else v
        if k.startswith(KV_PREFIX):
            out[k[len(KV_PREFIX) :]] = v
    return out


def dir_kv(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, dict[str, str]]:
    """file name -> its clinvar_gkm_parquet.* key-value metadata, for every .parquet in the directory."""
    return {p.name: read_kv(con, p) for p in sorted(Path(out_dir).glob("*.parquet"))}


def spec_for_dir(con: duckdb.DuckDBPyConnection, out_dir: Path, spec_arg: str | None = None) -> Spec:
    """The spec that wrote the files, from their key-value metadata (spec.spec_from_kv); every file must name the
    same one. A file with no table_spec key predates named specs and was written by clinvar-gkm. A custom spec
    needs `spec_arg` (--spec, its tables.yaml, whose sha256 must equal the recorded one); for a built-in one,
    `spec_arg` is optional and must name that spec. A directory with no files: `spec_arg`, else clinvar-gkm."""
    named = {
        tuple(kv.get(k) for k in SPEC_KV_KEYS)
        for kv in dir_kv(con, out_dir).values()
        if kv.get("table_spec_version") is not None
    }
    if not named:
        return resolve_spec(spec_arg)[0] if spec_arg else load_spec(TABLE_SPEC_VERSION)
    if len(named) > 1:
        labels = ", ".join(sorted(f"{n or PUBLISHED_SPEC} {v}" for n, v, *_ in named))
        raise SpecError(f"the files were written by different table specs: {labels}")
    kv = {k: v for k, v in zip(SPEC_KV_KEYS, named.pop(), strict=True) if v is not None}
    if "table_spec" not in kv:
        name, spec = PUBLISHED_SPEC, load_spec(kv["table_spec_version"])
    else:
        spec, source = spec_from_kv(kv, spec_arg)
        if not source.builtin:
            return spec
        name = source.name
    if spec_arg is not None:
        _, given = resolve_spec(spec_arg)
        version = kv["table_spec_version"]
        if (given.name, given.version, given.builtin) != (name, version, True):
            raise SpecError(f"--spec {spec_arg} is {given.label}, but the files were written by {name} {version}")
    return spec


def dir_selection(kvs: dict[str, dict[str, str]]) -> str | None:
    """The files' clinvar_gkm_parquet.selection entry (ADR 0004): None for a full build. Files that disagree
    (some with one, some without, or different ones) are an error: one directory holds one set."""
    values = {kv.get("selection") for kv in kvs.values()}
    if len(values) > 1:
        raise SpecError("the files disagree on clinvar_gkm_parquet.selection: they are not one set")
    return values.pop() if values else None


# --- verify_dir ------------------------------------------------------------------------------------------------


def verify_dir(out_dir: Path, spec: Spec | None = None) -> list[str]:
    """Problems found in a built directory on its own (every spec table present, metadata consistent, DESCRIBE
    equals the spec, the manifest matches the files). Empty list = pass."""
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        raise InputError(f"{out_dir}: not a directory")
    con = duckdb.connect()
    problems: list[str] = []
    files = {p.name[: -len(".parquet")]: p for p in sorted(out_dir.glob("*.parquet"))}
    if spec is None:
        try:
            spec = spec_for_dir(con, out_dir)
        except SpecError as e:
            return [f"can't load the table spec the files name: {e}"]
    spec_names = [t.name for t in spec.tables]
    for name in spec_names:
        if name not in files:
            problems.append(f"{name}: {name}.parquet is missing")
    for name in sorted(set(files) - set(spec_names)):
        problems.append(f"{name}: {name}.parquet is not a table of spec {spec.label}")

    kvs: dict[str, dict[str, str]] = {}
    for name in spec_names:
        if name not in files:
            continue
        path = files[name]
        got = describe_file(con, path)
        want = spec.table(name).describe()
        if got != want:
            problems.extend(f"{name}: schema: {d}" for d in _schema_diffs(got, want, "file", "spec"))
        kv = read_kv(con, path)
        kvs[name] = kv
        missing = [k for k in KV_KEYS if k not in kv]
        if missing:
            problems.append(f"{name}: key-value metadata lacks {', '.join(KV_PREFIX + k for k in missing)}")
        if kv.get("table", name) != name:
            problems.append(f"{name}: key-value metadata names table {kv['table']!r}")
        if "table_spec_version" in kv and kv["table_spec_version"] != spec.spec_version:
            problems.append(f"{name}: table_spec_version {kv['table_spec_version']} != spec {spec.spec_version}")
        if spec.name and kv.get("table_spec", spec.name) != spec.name:
            problems.append(f"{name}: table_spec {kv['table_spec']} != spec {spec.name}")
        if "bundle_schema_version" in kv and kv["bundle_schema_version"] not in spec.bundle_schema_versions:
            problems.append(
                f"{name}: bundle_schema_version {kv['bundle_schema_version']} is not one spec "
                f"{spec.spec_version} reads"
            )
        if "source_deltas" in kv:
            try:
                if not isinstance(json.loads(kv["source_deltas"]), list):
                    raise ValueError
            except ValueError:
                problems.append(f"{name}: source_deltas is not a JSON array: {_trunc(kv['source_deltas'])}")
    for k in KV_ORDER:
        if k == "table":
            continue
        # A required key missing from a file is reported above; an optional one (the spec's name, a custom
        # spec's keys, the selection) must be on every file or on none.
        values = {kv[k] for kv in kvs.values() if k in kv} if k in KV_KEYS else {kv.get(k) for kv in kvs.values()}
        if len(values) > 1:
            per = ", ".join(f"{n}={_trunc(kv.get(k))}" for n, kv in sorted(kvs.items()))
            problems.append(f"key-value metadata {KV_PREFIX}{k} differs across files: {per}")

    manifest_path = out_dir / MANIFEST_NAME
    if manifest_path.exists():
        selections = {kv.get("selection") for kv in kvs.values()}
        selection = selections.pop() if len(selections) == 1 else None
        problems.extend(_check_manifest(con, manifest_path, files, selection))
    con.close()
    return problems


def _check_manifest(con, path: Path, files: dict[str, Path], selection: str | None = None) -> list[str]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        tables = manifest["tables"]
    except (ValueError, KeyError, TypeError) as e:
        return [f"{path.name}: unreadable manifest ({e})"]
    problems = []
    want = json.loads(selection) if selection is not None else None
    if manifest.get("selection") != want:
        problems.append(f"{path.name}: selection {manifest.get('selection')} != the files' {want}")
    for name, entry in sorted(tables.items()):
        if name not in files:
            problems.append(f"{path.name}: lists {name}, but {name}.parquet is missing")
            continue
        rows = con.execute(f"SELECT count(*) FROM {_pq(files[name])}").fetchone()[0]
        if rows != entry.get("rows"):
            problems.append(f"{path.name}: {name} rows {entry.get('rows')} != file {rows}")
        sha = file_sha256(files[name])
        if sha != entry.get("sha256"):
            problems.append(f"{path.name}: {name} sha256 {entry.get('sha256')} != file {sha}")
    return problems


def _schema_diffs(a: list[tuple[str, str]], b: list[tuple[str, str]], an: str, bn: str) -> list[str]:
    da, db = dict(a), dict(b)
    out = []
    for c, _ in a:
        if c not in db:
            out.append(f"column {c} only in {an}")
        elif da[c] != db[c]:
            out.append(f"column {c}: {an} {da[c]} != {bn} {db[c]}")
    out.extend(f"column {c} only in {bn}" for c, _ in b if c not in da)
    common_a = [c for c, _ in a if c in db]
    common_b = [c for c, _ in b if c in da]
    if common_a != common_b:
        out.append(f"column order differs: {an} {common_a} vs {bn} {common_b}")
    return out


# --- bundle recompute ------------------------------------------------------------------------------------------


def recompute(con: duckdb.DuckDBPyConnection, spec: Spec, out_dir: Path, *, samples: int = SAMPLES) -> list[dict]:
    """Second leg of §8.2: per section group (one table, or the tables a row filter splits a section into), the
    id set equals the section keys and every `data` equals its bundle record. `con` holds `bundle_<section>`.

    Byte equality is checked by md5 of `data` against md5 of the record (a 128-bit digest), so the join carries
    two numbers per row, not the records themselves.
    """
    out_dir = Path(out_dir)
    names = [t.name for t in spec.tables]
    groups = table_groups(spec, names)
    seen: set[int] = set()
    results = []
    for name in names:
        g = groups[name]
        if id(g) in seen:
            continue
        seen.add(id(g))
        results.append(_recompute_group(con, spec, g.sections, g.tables, out_dir, samples))
    return results


def _recompute_group(con, spec: Spec, sections, table_names, out_dir: Path, samples: int) -> dict:
    tables = [spec.table(n) for n in table_names]
    res: dict = {"tables": list(table_names), "sections": list(sections), "problems": []}
    missing_files = [t.name for t in tables if not (out_dir / t.file_name).exists()]
    if missing_files:
        res["problems"].append(f"not checked: {', '.join(missing_files)} missing")
        return res
    no_key = [t.name for t in tables if key_column(t) is None]
    if no_key:
        res["problems"].append(f"not checked: {', '.join(no_key)} have no key column")
        return res
    with_record = all(record_column(t) for t in tables)
    union_t = " UNION ALL ".join(
        f"SELECT {sql_ident(key_column(t))} AS key, "
        f"{'md5_number(' + sql_ident(record_column(t)) + ')' if with_record else 'NULL::HUGEINT'} AS h, "
        f"{sql_str(t.name)} AS tbl FROM {_pq(out_dir / t.file_name)}"
        for t in tables
    )
    union_s = " UNION ALL ".join(
        f"SELECT key, {'md5_number(record)' if with_record else 'NULL::HUGEINT'} AS h FROM "
        f"{sql_ident('bundle_' + s)}"
        for s in sections
    )
    res["bundle_keys"] = con.execute(f"SELECT count(*) FROM ({union_s})").fetchone()[0]
    res["rows"] = dict(con.execute(f"SELECT tbl, count(*) FROM ({union_t}) GROUP BY tbl ORDER BY tbl").fetchall())
    for t in tables:
        res["rows"].setdefault(t.name, 0)
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _rc_diff AS
            SELECT coalesce(t.key, s.key) AS key, t.tbl,
                   CASE WHEN s.key IS NULL THEN 'not_in_bundle' WHEN t.key IS NULL THEN 'missing'
                        ELSE 'data_differs' END AS kind
            FROM ({union_t}) t FULL OUTER JOIN ({union_s}) s ON t.key = s.key
            WHERE s.key IS NULL OR t.key IS NULL OR t.h IS DISTINCT FROM s.h"""
    )
    for kind in ("not_in_bundle", "missing", "data_differs"):
        n = con.execute("SELECT count(*) FROM _rc_diff WHERE kind = ?", [kind]).fetchone()[0]
        rows = con.execute(
            f"SELECT key, tbl FROM _rc_diff WHERE kind = ? ORDER BY key, tbl LIMIT {int(samples)}", [kind]
        ).fetchall()
        res[kind] = {"count": n, "samples": [{"key": k, "table": tb} for k, tb in rows]}
    con.execute("DROP TABLE _rc_diff")
    dup = con.execute(
        f"""SELECT key, list(tbl ORDER BY tbl), count(*) OVER () FROM ({union_t})
            GROUP BY key HAVING count(*) > 1 ORDER BY key LIMIT {int(samples)}"""
    ).fetchall()
    res["duplicates"] = {
        "count": dup[0][2] if dup else 0,
        "samples": [{"key": k, "tables": tb} for k, tb, _ in dup],
    }
    if not with_record:
        res["problems"].append("data not compared: a table of the group has no record column")
    return res


def recompute_failed(group: dict) -> bool:
    if group.get("problems") and "bundle_keys" not in group:
        return True
    return any(group.get(k, {}).get("count") for k in ("not_in_bundle", "missing", "data_differs", "duplicates"))


def bundle_recompute(
    out_dir: Path,
    inputs: BuildInputs,
    *,
    spec: Spec,
    work_dir: Path | None = None,
    memory_limit: str | None = None,
    threads: int | None = None,
    log=None,
    samples: int = SAMPLES,
) -> list[dict]:
    """Stream and replay the bundle(s) as a build does (build.SectionPipeline) and, per section, compare the
    section's (key, record) set with the built tables' (id, data): `recompute`, one section at a time, with no
    state database."""
    out_dir = Path(out_dir)
    names = [t.name for t in spec.tables]
    groups: list = []
    for g in table_groups(spec, names).values():
        if not any(g is x for x in groups):
            groups.append(g)
    by_section: dict[str, list] = {}
    for g in groups:
        if len(g.sections) != 1:
            raise SpecError(f"table(s) {', '.join(g.tables)} read sections {list(g.sections)}; one is expected")
        by_section.setdefault(g.sections[0], []).append(g)
    found: dict[int, dict] = {}
    root = Path(work_dir) if work_dir else default_work_dir(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="verify-", dir=root))

    def process(con: duckdb.DuckDBPyConnection, section: str, rows: int) -> None:
        for g in by_section.get(section, []):
            found[id(g)] = _recompute_group(con, spec, g.sections, g.tables, out_dir, samples)

    pipe = SectionPipeline(
        inputs,
        spec,
        work,
        process=process,
        needed=set(by_section),
        memory_limit=memory_limit,
        threads=threads,
        log=log,
    )
    try:
        pipe.run()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    out = []
    for g in groups:
        res = found.get(id(g))
        if res is None:  # a spec section that is not a bundle section
            res = {
                "tables": list(g.tables),
                "sections": list(g.sections),
                "problems": [f"not checked: section(s) {', '.join(g.sections)} are not bundle sections"],
            }
        out.append(res)
    return out


# --- the oracle ------------------------------------------------------------------------------------------------


def _relation_types(con, path: Path) -> dict[str, duckdb.DuckDBPyType]:
    rel = con.sql(f"SELECT * FROM {_pq(path)} LIMIT 0")
    return dict(zip(rel.columns, rel.dtypes, strict=True))


def _nested_paths(t: duckdb.DuckDBPyType, steps: tuple = ()) -> Iterator[tuple]:
    """Leaf paths of a nested type, plus a NULL test at every struct/list and a length at every list."""
    if t.id == "struct":
        yield steps + ("#null",)
        for name, child in t.children:
            yield from _nested_paths(child, steps + (name,))
    elif t.id == "list":
        yield steps + ("#null",)
        yield steps + ("#len",)
        yield from _nested_paths(t.child, steps + ("[]",))
    else:
        yield steps


def _path_expr(base: str, steps: tuple, depth: int = 0) -> str:
    if not steps:
        return base
    step, rest = steps[0], steps[1:]
    if step == "#null":
        return f"(({base}) IS NULL)"
    if step == "#len":
        return f"len({base})"
    if step == "[]":
        v = f"_x{depth}"
        return f"list_transform({base}, lambda {v}: {_path_expr(v, rest, depth + 1)})"
    return _path_expr(f"struct_extract({base}, {sql_str(step)})", rest, depth)


def _path_label(col: str, steps: tuple) -> str:
    out = col
    for s in steps:
        out += {"#null": " (null)", "#len": " (length)", "[]": "[]"}.get(s, f".{s}")
    return out


def _json_first_diff(a, b, path: str = "$") -> str | None:
    """None if a and b are equal as JSON values (object key order ignored, bool distinct from number), else
    the path of the first difference."""
    if isinstance(a, bool) or isinstance(b, bool):
        return None if type(a) is type(b) and a == b else path
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return None if a == b else path
    if type(a) is not type(b):
        return path
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}.{k}"
            d = _json_first_diff(a[k], b[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path} (length {len(a)} vs {len(b)})"
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            d = _json_first_diff(x, y, f"{path}[{i}]")
            if d:
                return d
        return None
    return None if a == b else path


def _keep_smallest(samples: list, item, n: int) -> None:
    samples.append(item)
    if len(samples) > 2 * n:
        samples.sort(key=lambda s: s["id"])
        del samples[n:]


def compare_table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    tool_file: Path,
    ref_file: Path,
    table: Table | None,
    *,
    samples: int = SAMPLES,
) -> dict:
    """§8.2 items 1-5 for one table. `table` (the spec entry) names the id and record columns; without it the
    id column is `id` (or `key`) and the record columns are `data` / `value`."""
    res: dict = {"table": name}
    a_types, b_types = _relation_types(con, tool_file), _relation_types(con, ref_file)
    a_desc, b_desc = describe_file(con, tool_file), describe_file(con, ref_file)
    diffs = _schema_diffs(a_desc, b_desc, "tool", "reference")
    res["schema"] = {"equal": a_desc == b_desc, "differences": diffs, "tool": a_desc, "reference": b_desc}

    n_a = con.execute(f"SELECT count(*) FROM {_pq(tool_file)}").fetchone()[0]
    n_b = con.execute(f"SELECT count(*) FROM {_pq(ref_file)}").fetchone()[0]
    res["rows"] = {"tool": n_a, "reference": n_b, "equal": n_a == n_b}

    idc = key_column(table) if table else None
    if idc is None or idc not in a_types or idc not in b_types:
        idc = next((c for c in ("id", "key") if c in a_types and c in b_types), None)
    if idc is None:
        res["ids"] = {"column": None, "problem": "no shared id column"}
        return res
    qi = sql_ident(idc)
    ids: dict = {"column": idc}
    for side, (x, y) in {"only_tool": (tool_file, ref_file), "only_reference": (ref_file, tool_file)}.items():
        q = f"SELECT {qi} FROM {_pq(x)} EXCEPT SELECT {qi} FROM {_pq(y)}"
        cnt = con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0]
        smp = [r[0] for r in con.execute(f"SELECT * FROM ({q}) ORDER BY 1 LIMIT {int(samples)}").fetchall()]
        ids[side] = {"count": cnt, "samples": smp}
    for side, f in (("duplicates_tool", tool_file), ("duplicates_reference", ref_file)):
        ids[side] = con.execute(f"SELECT count(*) - count(DISTINCT {qi}) FROM {_pq(f)}").fetchone()[0]
    res["ids"] = ids

    common = [c for c in a_types if c in b_types and c != idc]
    if table:
        record_cols = [c.name for c in table.columns if c.derivation.kind == "record" and c.name in common]
    else:
        record_cols = [c for c in ("data", "value") if c in common]
    typed = [c for c in common if c not in record_cols]

    join = f"FROM {_pq(tool_file)} a JOIN {_pq(ref_file)} b ON a.{qi} = b.{qi}"

    def cmp(c: str) -> tuple[str, str]:
        qc = sql_ident(c)
        if str(a_types[c]) == str(b_types[c]):
            return f"a.{qc} IS DISTINCT FROM b.{qc}", "typed"
        return f"a.{qc} IS DISTINCT FROM TRY_CAST(b.{qc} AS {a_types[c]})", f"reference cast to {a_types[c]}"

    # One join for every typed column; record columns enter it as md5 digests, not as text, so the join's
    # payload stays small. Only the rows whose digests differ are fetched as text (_compare_records).
    def side(f: Path) -> str:
        cols = [f"{qi}"] + [sql_ident(c) for c in typed]
        cols += [f"md5_number({sql_ident(c)}::VARCHAR) AS {sql_ident('__h_' + c)}" for c in record_cols]
        return f"(SELECT {', '.join(cols)} FROM {_pq(f)})"

    hjoin = f"FROM {side(tool_file)} a JOIN {side(ref_file)} b ON a.{qi} = b.{qi}"
    exprs = [cmp(c)[0] for c in typed]
    exprs += [f"a.{sql_ident('__h_' + c)} IS DISTINCT FROM b.{sql_ident('__h_' + c)}" for c in record_cols]
    counts = con.execute("SELECT count(*)" + "".join(f", count_if({e})" for e in exprs) + f" {hjoin}").fetchone()
    res["joined_rows"] = counts[0]
    columns: dict = {}
    for c, n in zip(typed, counts[1 : 1 + len(typed)], strict=True):
        expr, how = cmp(c)
        entry: dict = {"distinct": n, "compared_as": how}
        if n:
            qc = sql_ident(c)
            rows = con.execute(
                f"SELECT a.{qi}, a.{qc}::VARCHAR, b.{qc}::VARCHAR {join} WHERE {expr} "
                f"ORDER BY a.{qi} LIMIT {int(samples)}"
            ).fetchall()
            entry["samples"] = [{"id": i, "tool": _trunc(x), "reference": _trunc(y)} for i, x, y in rows]
            if how == "typed" and a_types[c].id in ("struct", "list"):
                entry["paths"] = _path_breakdown(con, join, c, a_types[c])
        columns[c] = entry
    res["columns"] = columns

    records: dict = {}
    for c, n in zip(record_cols, counts[1 + len(typed) :], strict=True):
        entry = {
            "bytes_differ": n,
            "json_differ": 0,
            "format_only": 0,
            "samples_content": [],
            "samples_format": [],
        }
        if n:
            _compare_records(con, tool_file, ref_file, qi, c, entry, samples)
        records[c] = entry
    res["records"] = records
    return res


def _path_breakdown(con, join: str, col: str, t: duckdb.DuckDBPyType) -> dict[str, int]:
    qc = sql_ident(col)
    paths = list(_nested_paths(t))
    exprs = [f"count_if({_path_expr('a.' + qc, p)} IS DISTINCT FROM {_path_expr('b.' + qc, p)})" for p in paths]
    try:
        row = con.execute(f"SELECT {', '.join(exprs)} {join} WHERE a.{qc} IS DISTINCT FROM b.{qc}").fetchone()
    except duckdb.Error as e:  # a path DuckDB can't express; the column-level count still stands
        return {"(breakdown failed)": 0, "error": str(e)[:200]}
    return {_path_label(col, p): n for p, n in zip(paths, row, strict=True) if n}


def _compare_records(con, tool_file: Path, ref_file: Path, qi: str, col: str, entry: dict, samples: int) -> None:
    """Rows whose bytes differ, streamed: parsed-JSON equal (format only) or not (content). The differing ids
    are found from md5 digests first, so only those rows' text crosses the join."""
    qc = sql_ident(col)
    h = f"md5_number({qc}::VARCHAR)"
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _oracle_diff_ids AS
            SELECT a.{qi} AS id FROM (SELECT {qi}, {h} AS h FROM {_pq(tool_file)}) a
            JOIN (SELECT {qi}, {h} AS h FROM {_pq(ref_file)}) b ON a.{qi} = b.{qi}
            WHERE a.h IS DISTINCT FROM b.h"""
    )

    def only_diff(f: Path) -> str:
        return f"(SELECT {qi}, {qc}::VARCHAR AS v FROM {_pq(f)} WHERE {qi} IN (SELECT id FROM _oracle_diff_ids))"

    cur = con.execute(
        f"SELECT a.{qi}, a.v, b.v FROM {only_diff(tool_file)} a JOIN {only_diff(ref_file)} b ON a.{qi} = b.{qi}"
    )
    content, fmt = [], []
    while True:
        batch = cur.fetchmany(_BATCH)
        if not batch:
            break
        for i, x, y in batch:
            try:
                where = _json_first_diff(json.loads(x), json.loads(y)) if x is not None and y is not None else "$"
            except ValueError:
                where = "$ (invalid JSON)"
            item = {"id": i, "tool": _trunc(x), "reference": _trunc(y)}
            if where is None:
                entry["format_only"] += 1
                _keep_smallest(fmt, item, samples)
            else:
                entry["json_differ"] += 1
                item["path"] = where
                _keep_smallest(content, item, samples)
    entry["samples_content"] = sorted(content, key=lambda s: s["id"])[:samples]
    entry["samples_format"] = sorted(fmt, key=lambda s: s["id"])[:samples]
    con.execute("DROP TABLE _oracle_diff_ids")


def table_differences(r: dict) -> list[str]:
    """One line per kind of difference found in a compare_table result (empty = the table matches)."""
    if "missing" in r:
        return [r["missing"]]
    out = []
    if not r["schema"]["equal"]:
        out.append(f"schema ({len(r['schema']['differences'])} difference(s))")
    if not r["rows"]["equal"]:
        out.append(f"rows {r['rows']['tool']} vs {r['rows']['reference']}")
    ids = r.get("ids", {})
    if ids.get("column") is None:
        out.append("no shared id column")
        return out
    if ids["only_tool"]["count"]:
        out.append(f"{ids['only_tool']['count']} id(s) only in tool")
    if ids["only_reference"]["count"]:
        out.append(f"{ids['only_reference']['count']} id(s) only in reference")
    for side in ("duplicates_tool", "duplicates_reference"):
        if ids.get(side):
            out.append(f"{ids[side]} {side.replace('_', ' in ')}")
    for c, e in r.get("columns", {}).items():
        if e["distinct"]:
            out.append(f"{c}: {e['distinct']} row(s) differ")
    for c, e in r.get("records", {}).items():
        if e["bytes_differ"]:
            out.append(
                f"{c}: {e['bytes_differ']} byte difference(s) ({e['json_differ']} content, "
                f"{e['format_only']} format only)"
            )
    return out


def compare_dirs(
    con: duckdb.DuckDBPyConnection, tool_dir: Path, ref_dir: Path, spec: Spec, *, samples: int = SAMPLES
) -> dict:
    """The oracle: every table of the spec (and any other .parquet in either directory), one at a time."""
    tool_dir, ref_dir = Path(tool_dir), Path(ref_dir)
    names = [t.name for t in spec.tables]
    extra = sorted(
        {p.name[: -len(".parquet")] for d in (tool_dir, ref_dir) for p in d.glob("*.parquet")} - set(names)
    )
    tables = {}
    for name in names + extra:
        a, b = tool_dir / f"{name}.parquet", ref_dir / f"{name}.parquet"
        if not a.exists() or not b.exists():
            where = " and ".join(w for w, p in (("tool", a), ("reference", b)) if not p.exists())
            tables[name] = {"table": name, "missing": f"missing in {where}"}
        else:
            tbl = spec.table(name) if name in names else None
            tables[name] = compare_table(con, name, a, b, tbl, samples=samples)
        tables[name]["differences"] = table_differences(tables[name])
    return {"tool": str(tool_dir), "reference": str(ref_dir), "tables": tables}


# --- report ----------------------------------------------------------------------------------------------------


@dataclass
class VerifyReport:
    dir: str
    spec_version: str
    spec_problems: list[str] = field(default_factory=list)
    recompute: list[dict] | None = None
    oracle: dict | None = None
    spec_name: str | None = None
    selection: dict | None = None  # the files' selection entry (a subset), or None for a full build

    @property
    def passed(self) -> bool:
        if self.spec_problems:
            return False
        if self.recompute and any(recompute_failed(g) for g in self.recompute):
            return False
        if self.oracle and any(t["differences"] for t in self.oracle["tables"].values()):
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "dir": self.dir,
            "table_spec": self.spec_name,
            "table_spec_version": self.spec_version,
            "selection": self.selection,
            "passed": self.passed,
            "spec_check": {"problems": self.spec_problems},
            "bundle_recompute": self.recompute,
            "oracle": self.oracle,
        }

    def to_markdown(self) -> str:
        lines = [f"# clinvar-gkm-parquet verify: {self.dir}", ""]
        spec = f"{self.spec_name} {self.spec_version}" if self.spec_name else self.spec_version
        subset = ""
        if self.selection is not None:
            subset = (
                f" A selection: driver {self.selection.get('driver')}, {self.selection.get('selected_records')} "
                f"records, mini-bundle sha256 {self.selection.get('selected_sha256')}."
            )
        lines += [f"Table spec {spec}.{subset} Result: **{'pass' if self.passed else 'differences'}**.", ""]
        lines += ["## Directory check", ""]
        lines += [f"- {p}" for p in self.spec_problems] or ["No problems."]
        lines.append("")
        if self.recompute is not None:
            lines += [
                "## Bundle recompute",
                "",
                "| Tables | Bundle keys | Rows | Not in bundle | Missing | data differs | Duplicates |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
            for g in self.recompute:
                rows = sum(g.get("rows", {}).values())
                cells = [
                    str(g.get(k, {}).get("count", "-"))
                    for k in ("not_in_bundle", "missing", "data_differs", "duplicates")
                ]
                lines.append(
                    f"| {', '.join(g['tables'])} | {g.get('bundle_keys', '-')} | {rows} | "
                    + " | ".join(cells)
                    + " |"
                )
            lines.append("")
            for g in self.recompute:
                if not recompute_failed(g) and not g.get("problems"):
                    continue
                lines += [f"### {', '.join(g['tables'])}", ""]
                lines += [f"- {p}" for p in g.get("problems", [])]
                for k in ("not_in_bundle", "missing", "data_differs", "duplicates"):
                    e = g.get(k)
                    if e and e["count"]:
                        lines.append(f"- {k}: {e['count']}; e.g. {', '.join(json.dumps(s) for s in e['samples'])}")
                lines.append("")
        if self.oracle is not None:
            lines += [
                "## Oracle",
                "",
                f"Tool `{self.oracle['tool']}` against reference `{self.oracle['reference']}`.",
                "",
                "| Table | Result |",
                "| --- | --- |",
            ]
            for name, t in self.oracle["tables"].items():
                lines.append(f"| {name} | {'; '.join(t['differences']) or 'match'} |")
            lines.append("")
            for name, t in self.oracle["tables"].items():
                if t["differences"]:
                    lines += _oracle_table_md(name, t)
        return "\n".join(lines).rstrip() + "\n"


def _oracle_table_md(name: str, t: dict) -> list[str]:
    lines = [f"### {name}", ""]
    if "missing" in t:
        return lines + [f"- {t['missing']}", ""]
    for d in t["schema"]["differences"]:
        lines.append(f"- schema: {d}")
    if not t["rows"]["equal"]:
        lines.append(f"- rows: tool {t['rows']['tool']}, reference {t['rows']['reference']}")
    ids = t.get("ids", {})
    for side in ("only_tool", "only_reference"):
        if ids.get(side, {}).get("count"):
            lines.append(
                f"- ids {side.replace('_', ' in ')}: {ids[side]['count']}; e.g. "
                f"{', '.join(map(str, ids[side]['samples']))}"
            )
    for c, e in t.get("columns", {}).items():
        if not e["distinct"]:
            continue
        lines.append(f"- column `{c}` ({e['compared_as']}): {e['distinct']} row(s) differ")
        for p, n in e.get("paths", {}).items():
            lines.append(f"  - `{p}`: {n}")
        for s in e.get("samples", []):
            lines.append(f"  - e.g. {s['id']}: tool `{s['tool']}` / reference `{s['reference']}`")
    for c, e in t.get("records", {}).items():
        if not e["bytes_differ"]:
            continue
        lines.append(
            f"- `{c}`: {e['bytes_differ']} byte difference(s): {e['json_differ']} content, "
            f"{e['format_only']} format only"
        )
        for s in e["samples_content"]:
            lines.append(f"  - content, e.g. {s['id']} at `{s['path']}`")
        for s in e["samples_format"]:
            lines.append(f"  - format only, e.g. {s['id']}")
    return lines + [""]


def write_report(report: VerifyReport, path: Path) -> tuple[Path, Path]:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_markdown(), encoding="utf-8")
    jpath = path.with_suffix(".json")
    jpath.write_text(json.dumps(report.to_dict(), indent=2, default=str) + "\n", encoding="utf-8")
    return path, jpath


# --- CLI -------------------------------------------------------------------------------------------------------


def _usage(msg: str) -> int:
    print(f"clinvar-gkm-parquet verify: error: {msg}", file=sys.stderr)
    return 2


def run_verify_command(args) -> int:
    """`clinvar-gkm-parquet verify`: returns the exit code."""
    out_dir = Path(args.dir)
    if not out_dir.is_dir():
        return _usage(f"{out_dir}: not a directory")
    if args.bundle_recompute and not args.full:
        return _usage("--bundle-recompute needs --full")
    if (args.full or args.delta or args.manifest) and not args.bundle_recompute:
        return _usage("--full/--delta/--manifest are only read with --bundle-recompute")
    if args.against is not None and not Path(args.against).is_dir():
        return _usage(f"--against {args.against}: not a directory")

    meta = duckdb.connect()
    spec = spec_for_dir(meta, out_dir, getattr(args, "spec", None))
    kvs = dir_kv(meta, out_dir)
    meta.close()
    selection = dir_selection(kvs)
    params = json.loads(selection) if selection is not None else None
    if args.against is not None:
        # The oracle compares a full clinvar-gkm build with ClinVar-GKM's published set (design plan §8.2).
        if params is not None:
            return _usage(
                f"--against compares a full build with the published set; {out_dir} is a selection (driver "
                f"{params.get('driver')}, clinvar_gkm_parquet.selection)"
            )
        written_by = {
            (kv.get("table_spec", PUBLISHED_SPEC), kv.get("table_spec_builtin", "true")) for kv in kvs.values()
        }
        if any(name != PUBLISHED_SPEC or builtin == "false" for name, builtin in written_by):
            names = ", ".join(sorted(n + (" (custom)" if b == "false" else "") for n, b in written_by))
            return _usage(
                f"--against needs a set built with the built-in {PUBLISHED_SPEC} spec; {out_dir} has {names}"
            )
    report = VerifyReport(dir=str(out_dir), spec_version=spec.spec_version, spec_name=spec.name, selection=params)
    report.spec_problems = verify_dir(out_dir, spec)
    if args.bundle_recompute:
        if params is None:
            inputs = resolve_build_inputs(args)
        else:
            # A selection is recomputed against its mini-bundle (ADR 0004): `--full` names it, and its sha256
            # must be the recorded selected_sha256. `clinvar-gkm-parquet select` with the recorded options
            # regenerates it byte for byte.
            expected = params["selected_sha256"]
            if args.delta or args.manifest:
                return _usage(
                    "these files are a selection: recompute against the mini-bundle alone (--full <mini-bundle>, "
                    "no --delta); it holds the replayed state. Regenerate it with `clinvar-gkm-parquet select` "
                    "and the options recorded in clinvar_gkm_parquet.selection"
                )
            if args.sha256 and args.sha256.lower() != expected:
                return _usage(f"--sha256 {args.sha256} is not the selection's selected_sha256 {expected}")
            inputs = BuildInputs(full=sources.resolve_input(args.full, sha256=expected), full_source_release=None)
        try:
            report.recompute = bundle_recompute(
                out_dir,
                inputs,
                spec=spec,
                work_dir=args.work_dir,
                memory_limit=args.memory_limit,
                threads=args.threads,
                log=lambda m: print(f"clinvar-gkm-parquet: {m}", file=sys.stderr, flush=True),
            )
        except InputError as e:
            if params is not None and "sha256 mismatch" in str(e):
                raise InputError(
                    f"--full {args.full} is not the mini-bundle these files were built from (selected_sha256 "
                    f"{params['selected_sha256']}): {e}"
                ) from e
            raise
    if args.against is not None:
        root = Path(args.work_dir) if args.work_dir else None
        tmp = Path(tempfile.mkdtemp(prefix="clinvar-gkm-parquet-oracle-", dir=root))
        try:
            con = duckdb.connect()
            configure(con, memory_limit=args.memory_limit, threads=args.threads, temp_directory=tmp)
            report.oracle = compare_dirs(con, out_dir, Path(args.against), spec)
            con.close()
        except duckdb.Error as e:
            raise ToolError(f"oracle comparison failed: {e}") from e
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    if args.report:
        md, js = write_report(report, args.report)
        print(f"report: {md} and {js}")
    print(_summary(report))
    return 0 if report.passed else 1


def _summary(r: VerifyReport) -> str:
    parts = [f"directory check: {len(r.spec_problems)} problem(s)"]
    if r.recompute is not None:
        parts.append(f"bundle recompute: {sum(recompute_failed(g) for g in r.recompute)} group(s) differ")
    if r.oracle is not None:
        parts.append(f"oracle: {sum(bool(t['differences']) for t in r.oracle['tables'].values())} table(s) differ")
    lines = [f"verify {'pass' if r.passed else 'DIFFERENCES'}: " + "; ".join(parts)]
    lines += [f"  {p}" for p in r.spec_problems[:20]]
    return "\n".join(lines)
