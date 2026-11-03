"""The round trip (ADR 0005): a row back to its bundle record, and a lossless build back to its bundle text.

A table with a `json` block (spec.py) states how each row is written back as its bundle record, byte for byte:
which column is which property, the key order, where pointers point. `record_sql(table)` turns that into one SQL
expression over the table's columns. It serves twice:

- The build's check (`checked_sql`, called by sqltemplate.render_generic): the table's SQL also carries the
  bundle line, and every row's record, written back from its columns, must equal it (the key's JSON text too), or
  the build stops naming the key and the first differing character. So a record the columns can't reproduce
  (a JSON null, a float, a key order the layout doesn't state, an escape the writer doesn't make) never makes it
  into a lossless build.
- `rebuild`: a full build of a `lossless` spec (the default, sections) back to the bundle's uncompressed text,
  section by section in the bundle schema's order, records in `ordinal` order, in the assembler's layout
  (bundle.py). Each section's record lines are hashed as they are written and compared with the file's
  `section_text_sha256`; the whole text with the manifest's `source_text_sha256`. Anything else stops it: a
  spec that is not lossless, a selection, deltas, a missing table or manifest, ordinals that are not 0..n-1, a
  pointer section outside the column's targets, a hash that differs.

The JSON text: strings by `cgp_jstr` (DuckDB's to_json with Python's lower-case \\u escapes: the producer writes
with Python's json.dumps, ensure_ascii=False), integers as their decimal text, booleans as true/false. An object
leaves out NULL members, so NULL is absent; a NULL list element is `null`. A single-target pointer is
`#/<target>/<id>`; a multi-target one takes its section from the pointer-section column of the same path.

DuckDB pastes macro arguments into the macro body, so the expression is built here in Python and every macro it
calls gets column references, struct fields or lambda variables only (a nested expression passed to a macro that
uses its argument twice doubles at every level).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .compat import SCHEMA_SECTIONS
from .errors import RoundTripError, SpecError
from .spec import RANGE_KINDS, Column, Spec, Table

RECORD_COLUMN = "__record"  # the bundle record, carried by a json table's SQL for the check
KEY_JSON_COLUMN = "__key_json"  # the bundle key as written (its JSON text)
LINE_COLUMN = "__line"
_FETCH = 20_000  # rows per fetch while writing
_PROPERTY_PATH = re.compile(r"\$\.(\w+)(?:\[\*\])?")
_INTEGERS = ("bigint", "integer", "smallint", "tinyint", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint")


def _ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _key_bytes(k: str) -> bytes:
    return k.encode("utf-8")


# --- one row back to its record --------------------------------------------------------------------------------


class _Writer:
    """Builds one table's record expression; `ref(column)` is how a column is read."""

    def __init__(self, table: Table, ref: Callable[[str], str]):
        if table.json is None:
            raise SpecError(f"table {table.name}: no json block: its rows can't be written back as records")
        self.table = table
        self.ref = ref
        self.n = 0
        keys = table.columns_of("key")
        if len(keys) != 1:
            raise SpecError(f"table {table.name}: the round trip needs one key column")
        self.key = ref(keys[0].name)

    def var(self) -> str:
        self.n += 1
        return f"__e{self.n}"

    def ctx(self, c: Column) -> str:
        return _lit(f"table {self.table.name}: column {c.name}")

    # values -----------------------------------------------------------------------------------------------------

    def obj(self, parts: list[tuple[str, str]], sort: bool) -> str:
        if sort:
            parts = sorted(parts, key=lambda p: _key_bytes(p[0]))
        names = [n for n, _ in parts]
        if len(set(names)) != len(names):
            raise SpecError(f"table {self.table.name}: two members named alike in {names}")
        members = ", ".join(f"{_lit(json.dumps(n, ensure_ascii=False) + ':')} || {v}" for n, v in parts)
        return f"'{{' || concat_ws(',', {members}) || '}}'"

    def value(self, x: str, t, rel: str, c: Column, sort: bool, ptrs: dict[str, tuple[str, ...]]) -> str:
        """JSON text of x, of DuckDB type t, at position rel of column c's value (NULL stays NULL)."""
        tid = t.id
        if tid == "struct":
            return f"CASE WHEN {x} IS NULL THEN NULL ELSE {self.struct(x, t, rel, c, sort, ptrs)} END"
        if tid == "list":
            e = self.var()
            elem = self.value(e, t.children[0][1], f"{rel}[]", c, sort, ptrs)
            return (
                f"CASE WHEN {x} IS NULL THEN NULL ELSE '[' || array_to_string(list_transform({x}, "
                f"lambda {e}: coalesce({elem}, 'null')), ',') || ']' END"
            )
        if rel in ptrs:
            targets = ptrs[rel]
            if tid != "varchar" or len(targets) != 1:
                raise SpecError(
                    f"table {self.table.name}: column {c.name}: pointer at {rel or '(column)'} needs VARCHAR and "
                    f"one target section (the round trip can't place a multi-target pointer inside a value)"
                )
            return f"cgp_jstr({_lit('#/' + targets[0] + '/')} || {x})"
        if tid == "varchar":
            return f"cgp_jstr({x})"
        if tid in _INTEGERS:
            return f"CAST({x} AS VARCHAR)"
        if tid == "boolean":
            return f"CASE WHEN {x} THEN 'true' WHEN NOT {x} THEN 'false' END"
        raise SpecError(
            f"table {self.table.name}: column {c.name}: no exact JSON text for type {t} at {rel or '(column)'} "
            "(the round trip writes VARCHAR, integers and BOOLEAN)"
        )

    def struct(self, x: str, t, rel: str, c: Column, sort: bool, ptrs) -> str:
        fields = list(t.children)
        names = [f for f, _ in fields]
        slots = [(f, ft) for f, ft in fields if f.startswith("value_")]
        parts: list[tuple[str, str]] = []
        if "name" in names and slots:  # an extension: its value_* slots are one `value`
            if self.table.extension_slots.get(c.name) != "json-type":
                raise SpecError(
                    f"table {self.table.name}: column {c.name}: the round trip needs json-type extension slots "
                    "(with slots by name, the value's JSON kind is not known)"
                )
            values = [self.value(f"{x}.{_ident(f)}", ft, f"{rel}.{f}", c, sort, ptrs) for f, ft in slots]
            # Every slot NULL: the value was [] (an empty array fills no slot; a null or absent value stops the
            # build), possible only where the struct has an array slot.
            if any(ft.id == "list" for _, ft in slots):
                values.append("'[]'")
            for f, ft in fields:
                if f == slots[0][0]:
                    parts.append(("value", f"coalesce({', '.join(values)})"))
                elif not f.startswith("value_"):
                    parts.append((f, self.value(f"{x}.{_ident(f)}", ft, f"{rel}.{f}", c, sort, ptrs)))
        else:
            for f, ft in fields:
                parts.append((f, self.value(f"{x}.{_ident(f)}", ft, f"{rel}.{f}", c, sort, ptrs)))
        return self.obj(parts, sort)

    # the record -------------------------------------------------------------------------------------------------

    def record(self) -> str:
        from .sqltemplate import _parse_type

        table, layout = self.table, self.table.json
        sort_all = layout.key_order == "sorted"
        sections = {c.derivation.path: c for c in table.columns_of("pointer-section")}
        used_sections: set[str] = set()
        ranges: dict[str, dict[str, Column]] = {}
        for c in table.columns:
            if c.derivation.kind in RANGE_KINDS:
                ranges.setdefault(c.derivation.path, {})[c.derivation.kind] = c
        parts: list[tuple[str, str]] = []
        for c in table.columns:
            kind, path = c.derivation.kind, c.derivation.path
            where = f"table {table.name}: column {c.name}"
            if kind == "key":
                if layout.record_id:
                    parts.append(("id", f"cgp_jstr({self.ref(c.name)})"))
                continue
            if kind in ("ordinal", "pointer-section"):
                continue
            m = _PROPERTY_PATH.fullmatch(path or "")
            if not m:
                raise SpecError(f"{where}: the round trip needs a top-level path ($.<field>), not {path}")
            prop = m.group(1)
            sort = sort_all or c.name in layout.sorted_columns
            t = _parse_type(c.type)
            if kind in RANGE_KINDS:
                group = ranges[path]
                if c is not next(iter(group.values())):
                    continue
                if set(group) != set(RANGE_KINDS):
                    raise SpecError(
                        f"{where}: path {path} needs all of {', '.join(RANGE_KINDS)} to be written back"
                    )
                ex, lo, hi = (self.ref(group[k].name) for k in RANGE_KINDS)
                parts.append(
                    (
                        prop,
                        f"CASE WHEN {ex} IS NOT NULL THEN CAST({ex} AS VARCHAR) "
                        f"WHEN {lo} IS NULL AND {hi} IS NULL THEN NULL "
                        f"ELSE '[' || coalesce(CAST({lo} AS VARCHAR), 'null') || ',' || "
                        f"coalesce(CAST({hi} AS VARCHAR), 'null') || ']' END",
                    )
                )
                continue
            if c.nulls != "when-absent":
                raise SpecError(f"{where}: the round trip needs nulls: when-absent (NULL = absent)")
            if kind == "scalar-path":
                parts.append((prop, self.value(self.ref(c.name), t, "", c, sort, {})))
            elif kind == "bare-id":
                parts.append((prop, self.bare_id(c, t, sections, used_sections)))
            elif kind in ("typed-copy", "extension-slot"):
                d = c.derivation
                if d.pointers and not d.pointer_targets:
                    raise SpecError(f"{where}: pointers need their target sections ({{position: [sections]}})")
                ptrs = {p.replace("[*]", "[]"): ts for p, ts in zip(d.pointers, d.pointer_targets, strict=True)}
                parts.append((prop, self.value(self.ref(c.name), t, "", c, sort, ptrs)))
            else:
                raise SpecError(f"{where}: the round trip can't write back a {kind} column")
        unused = sorted(set(sections) - used_sections)
        if unused:
            raise SpecError(
                f"table {table.name}: pointer-section column(s) of path(s) {unused} have no multi-target bare-id "
                "column of the same path"
            )
        return self.obj(parts, sort_all)

    def bare_id(self, c: Column, t, sections: dict[str, Column], used: set[str]) -> str:
        targets = c.derivation.targets
        where = f"table {self.table.name}: column {c.name}"
        if not targets:
            raise SpecError(f"{where}: a bare-id column needs its target section(s) (targets) to be written back")
        x = self.ref(c.name)
        is_list = t.id == "list"
        if len(targets) == 1:
            prefix = _lit(f"#/{targets[0]}/")
            if not is_list:
                return f"cgp_jstr({prefix} || {x})"
            e = self.var()
            return (
                f"CASE WHEN {x} IS NULL THEN NULL ELSE '[' || array_to_string(list_transform({x}, "
                f"lambda {e}: coalesce(cgp_jstr({prefix} || {e}), 'null')), ',') || ']' END"
            )
        sc = sections.get(c.derivation.path)
        if sc is None or sc.type != c.type:
            raise SpecError(
                f"{where}: a pointer with several targets ({', '.join(targets)}) needs a pointer-section column "
                f"of the same path and type ({c.derivation.path}, {c.type})"
            )
        if sc.derivation.targets and set(sc.derivation.targets) != set(targets):
            raise SpecError(f"{where}: pointer-section column {sc.name} names other targets")
        used.add(c.derivation.path)
        s = self.ref(sc.name)
        tl = "[" + ", ".join(_lit(x) for x in targets) + "]"
        ctx = self.ctx(c)
        if not is_list:
            sec = f"cgp_ptr_section({s}, {x}, {tl}, {ctx}, {self.key})"
            return f"cgp_jstr('#/' || {sec} || '/' || {x})"
        e, i = self.var(), self.var()
        sec = f"cgp_ptr_section({s}[{i}], {e}, {tl}, {ctx}, {self.key})"
        return (
            f"CASE WHEN {x} IS NULL THEN NULL "
            f"WHEN len({x}) IS DISTINCT FROM len({s}) THEN cgp_fail({ctx}, {self.key}, "
            f"{_lit(f'{c.name} and {sc.name} differ in length')}) "
            f"ELSE '[' || array_to_string(list_transform({x}, "
            f"lambda {e}, {i}: coalesce(cgp_jstr('#/' || {sec} || '/' || {e}), 'null')), ',') || ']' END"
        )


def record_sql(table: Table, ref: Callable[[str], str] = _ident) -> str:
    """The SQL of a row's bundle record (its JSON text) from the table's columns (read as `ref(name)`)."""
    return _Writer(table, ref).record()


def line_sql(table: Table, ref: Callable[[str], str] = _ident) -> str:
    """The SQL of the row's record line as the bundle writes it, without the indent and the trailing comma:
    `"<key>": <record>`."""
    w = _Writer(table, ref)
    return f"{'cgp_jstr(' + w.key + ')'} || ': ' || {w.record()}"


def checked_sql(table: Table, sql: str) -> str:
    """The table's SQL (with RECORD_COLUMN and KEY_JSON_COLUMN) wrapped in the round-trip check: every row's line
    written back must equal the bundle line, else the build stops. The output columns are the table's."""
    key = _ident(table.columns_of("key")[0].name)
    line, orig = _ident(LINE_COLUMN), f"{_ident(KEY_JSON_COLUMN)} || ': ' || {_ident(RECORD_COLUMN)}"
    hidden = ", ".join(_ident(c) for c in (RECORD_COLUMN, KEY_JSON_COLUMN, LINE_COLUMN))
    ctx = _lit(f"table {table.name}: round trip")
    return (
        f"SELECT * EXCLUDE ({hidden}) FROM (\n"
        f"SELECT *, {line_sql(table)} AS {line} FROM (\n{sql}\n) AS __rt\n) AS __rc\n"
        f"WHERE CASE WHEN {line} = {orig} THEN TRUE ELSE cgp_roundtrip_fail({ctx}, {key}, {line}, {orig}) END"
    )


def lossless_problems(spec: Spec) -> list[str]:
    """What keeps the spec's tables from being written back (every table with a json block), and, for a
    `lossless` spec, from being a lossless encoding of the bundle text (spec.py). Empty: nothing."""
    out: list[str] = []
    for t in spec.tables:
        if t.json is None:
            continue
        try:
            record_sql(t)
        except SpecError as e:
            out.append(str(e))
        for c in t.columns:
            if c.name in (RECORD_COLUMN, KEY_JSON_COLUMN, LINE_COLUMN):
                out.append(f"table {t.name}: column name {c.name} is reserved for the round-trip check")
    if not spec.lossless:
        return out
    for v in spec.bundle_schema_versions:
        want = SCHEMA_SECTIONS.get(v, ())
        have = [s for t in spec.tables for s in t.sections]
        if sorted(have) != sorted(want) or len(have) != len(spec.tables):
            out.append(
                f"lossless: the tables must read each section of bundle schema {v} once, one section a table "
                f"(missing: {sorted(set(want) - set(have))}, extra or repeated: "
                f"{sorted(s for s in set(have) if have.count(s) > 1 or s not in want)})"
            )
    for t in spec.tables:
        tw = f"lossless: table {t.name}"
        if t.json is None:
            out.append(f"{tw}: needs a json block")
        if t.row_filter is not None:
            out.append(f"{tw}: has a row filter")
        if not t.known_fields:
            out.append(f"{tw}: needs known_fields: true")
        ords = t.columns_of("ordinal")
        if len(ords) != 1 or t.columns[-1] is not ords[0]:
            out.append(f"{tw}: needs one ordinal column, as its last column")
    return out


# --- a build back to its bundle text ---------------------------------------------------------------------------


@dataclass
class RebuildResult:
    out: Path | None  # None: the text was hashed, not written
    schema_version: str
    text_sha256: str
    text_size: int
    expected_text_sha256: str  # the manifest's source_text_sha256
    sections: dict[str, int] = field(default_factory=dict)  # section -> records written


class RoundTripMismatch(RoundTripError):
    """The rebuilt text differs from the recorded one: a section's sha256, or the whole text's."""


class _Out:
    """Where the text goes: a file (gzip when the name ends in .gz), or nowhere; hashed either way."""

    def __init__(self, path: Path | None, compresslevel: int, gz: bool):
        self.sha = hashlib.sha256()
        self.size = 0
        self._fh = self._gz = None
        if path is not None:
            self._fh = open(path, "wb")
            if gz:
                self._gz = gzip.GzipFile(
                    filename="", mode="wb", fileobj=self._fh, compresslevel=compresslevel, mtime=0
                )

    def write(self, b: bytes) -> None:
        self.sha.update(b)
        self.size += len(b)
        if self._gz is not None:
            self._gz.write(b)
        elif self._fh is not None:
            self._fh.write(b)

    def close(self) -> None:
        try:
            if self._gz is not None:
                self._gz.close()
        finally:
            if self._fh is not None:
                self._fh.close()


def _shared_kv(kvs: dict[str, dict[str, str]], key: str) -> str | None:
    values = {kv.get(key) for kv in kvs.values()}
    if len(values) != 1:
        raise RoundTripError(f"the files disagree on clinvar_gkm_parquet.{key}: they are not one build")
    return values.pop()


def rebuild(
    out_dir: Path,
    out: Path | None = None,
    *,
    spec_arg: str | None = None,
    memory_limit: str | None = None,
    threads: int | None = None,
    work_dir: Path | None = None,
    compresslevel: int = 6,
    log: Callable[[str], None] | None = None,
) -> RebuildResult:
    """Write the bundle a full lossless build was made from (`out`, gzip when it ends in .gz; None: hash only)
    and check it against the recorded sha256s (module docstring). Returns what was written; raises
    RoundTripError (RoundTripMismatch for a differing text) and leaves no output file on any failure."""
    from .build import EMPTY_SHA256, MANIFEST_NAME, configure
    from .sqltemplate import macros_sql
    from .verify import dir_kv, spec_for_dir

    log = log or (lambda m: None)
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        raise RoundTripError(f"{out_dir}: not a directory")
    meta = duckdb.connect()
    try:
        spec = spec_for_dir(meta, out_dir, spec_arg)
        kvs = dir_kv(meta, out_dir)
    finally:
        meta.close()
    if not spec.lossless:
        raise RoundTripError(
            f"{out_dir}: written by table spec {spec.label}, which is not a lossless encoding of the bundle "
            "(lossless: true); only a full build of such a spec (the default, sections) can be rebuilt"
        )
    files = {name[: -len(".parquet")]: out_dir / name for name in kvs}
    missing = [t.name for t in spec.tables if t.name not in files]
    if missing:
        raise RoundTripError(f"{out_dir}: missing table(s) {', '.join(missing)}: a rebuild needs every table")
    extra = sorted(set(files) - {t.name for t in spec.tables})
    if extra:
        raise RoundTripError(f"{out_dir}: file(s) of no table of spec {spec.label}: {', '.join(extra)}")
    if any("selection" in kv for kv in kvs.values()):
        raise RoundTripError(f"{out_dir}: a selective build (clinvar_gkm_parquet.selection) is not a whole bundle")
    if _shared_kv(kvs, "source_deltas") != "[]":
        raise RoundTripError(
            f"{out_dir}: built with deltas replayed: the tables hold a state no one bundle text holds"
        )
    no_text = [n for n, kv in kvs.items() if "section_text_sha256" not in kv]
    if no_text:
        raise RoundTripError(
            f"{out_dir}: {', '.join(sorted(no_text)[:5])} carry no clinvar_gkm_parquet.section_text_sha256: not "
            "a lossless build (a selection or a build with deltas records none)"
        )
    for k in ("tool_version", "table_spec", "table_spec_version", "bundle_schema_version", "source_bundle_sha256"):
        _shared_kv(kvs, k)
    version = _shared_kv(kvs, "bundle_schema_version")
    if version not in spec.bundle_schema_versions or version not in SCHEMA_SECTIONS:
        raise RoundTripError(f"{out_dir}: bundle_schema_version {version} is not one spec {spec.label} reads")
    manifest_path = out_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise RoundTripError(
            f"{manifest_path}: missing; a rebuild checks the whole text against its source_text_sha256"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except ValueError as e:
        raise RoundTripError(f"{manifest_path}: unreadable manifest ({e})") from e
    expected_text = manifest.get("source_text_sha256")
    if expected_text is None:
        raise RoundTripError(f"{manifest_path}: no source_text_sha256, but the files are a lossless build")
    if manifest.get("source_bundle_sha256") != _shared_kv(kvs, "source_bundle_sha256"):
        raise RoundTripError(f"{manifest_path}: source_bundle_sha256 differs from the files'")

    tmp = None
    if out is not None:
        out = Path(out)
        if out.exists():
            raise RoundTripError(f"{out}: already exists")
        tmp = out.with_name(f".{out.name}.{os.getpid()}.partial")
    spill = Path(tempfile.mkdtemp(prefix="clinvar-gkm-parquet-rebuild-", dir=work_dir))
    con = duckdb.connect()
    try:
        configure(con, memory_limit=memory_limit, threads=threads, temp_directory=spill)
        con.execute(macros_sql())
        by_section = {t.sections[0]: t for t in spec.tables}
        rows = {
            s: _open_table(con, by_section[s], files[by_section[s].name], spec) for s in SCHEMA_SECTIONS[version]
        }
        for s in SCHEMA_SECTIONS[version]:
            if not rows[s] and kvs[files[by_section[s].name].name]["section_text_sha256"] != EMPTY_SHA256:
                raise RoundTripMismatch(f"section {s}: no rows, but the build recorded a non-empty section text")
        present = [s for s in SCHEMA_SECTIONS[version] if rows[s]]
        want = {s: kvs[files[by_section[s].name].name]["section_text_sha256"] for s in present}
        sink = _Out(tmp, compresslevel, gz=out is not None and out.name.endswith(".gz"))
        ok = False
        try:
            _write_text(con, sink, version, [(s, by_section[s], rows[s], want[s]) for s in present], log)
            sink.close()
            got = sink.sha.hexdigest()
            if got != expected_text:
                raise RoundTripMismatch(
                    f"the rebuilt text has sha256 {got}, the manifest records source_text_sha256 {expected_text}"
                )
            if tmp is not None:
                os.replace(tmp, out)
            ok = True
        finally:
            if not ok:
                try:
                    sink.close()
                except OSError:
                    pass
                if tmp is not None:
                    tmp.unlink(missing_ok=True)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)
    return RebuildResult(
        out=out,
        schema_version=version,
        text_sha256=sink.sha.hexdigest(),
        text_size=sink.size,
        expected_text_sha256=expected_text,
        sections={s: rows[s] for s in present},
    )


class _TextSink:
    """A bundle.stream_bundle sink that only takes the text digests."""

    def __init__(self):
        self.sections: dict[str, str] = {}

    def begin(self, schema_version: str) -> None:
        pass

    def open_section(self, section: str):
        return lambda key, record: None

    def section_text(self, section: str, sha256: str) -> None:
        self.sections[section] = sha256

    def close_section(self, section: str) -> None:
        pass


def _open_table(con: duckdb.DuckDBPyConnection, t: Table, path: Path, spec: Spec) -> int:
    """A view of the table's file, checked: the spec's columns, ordinals 0..n-1. Returns n."""
    from .build import describe_file, sql_str

    if describe_file(con, path) != t.describe():
        raise RoundTripError(f"{path.name}: its columns are not table {t.name}'s of spec {spec.label}")
    con.execute(f"CREATE VIEW {_ident(t.name)} AS SELECT * FROM read_parquet({sql_str(str(path))})")
    o = _ident(t.columns_of("ordinal")[0].name)
    n, n_o, distinct, lo, hi = con.execute(
        f"SELECT count(*), count({o}), count(DISTINCT {o}), min({o}), max({o}) FROM {_ident(t.name)}"
    ).fetchone()
    if n and (n_o, distinct, lo, hi) != (n, n, 0, n - 1):
        raise RoundTripError(
            f"table {t.name}: the ordinals are not 0..{n - 1} ({n} rows, {n_o} ordinals, {distinct} distinct, "
            f"min {lo}, max {hi}): the record order can't be restored"
        )
    return n


def _write_text(con, sink: _Out, version: str, sections: list[tuple[str, Table, int, str]], log) -> None:
    """The bundle text in the assembler's layout (bundle.py): (section, table, rows, recorded sha256) per present
    section, in order. Each section's record lines are hashed as written and compared with the recorded hash."""
    sink.write(f'{{\n  "schema_version": "{version}"'.encode() + (b",\n" if sections else b"\n"))
    for i, (section, t, n, want) in enumerate(sections):
        log(f"section {section}: {n} records")
        sink.write(f'  "{section}": {{\n'.encode())
        sec = hashlib.sha256()
        first, written = True, 0
        for lines in _record_lines(con, t):
            b = (("" if first else ",\n") + ",\n".join(lines)).encode("utf-8")
            first = False
            sec.update(b)
            sink.write(b)
            written += len(lines)
        sec.update(b"\n")
        sink.write(b"\n")
        if written != n:
            raise RoundTripError(f"table {t.name}: wrote {written} records of {n}")
        if sec.hexdigest() != want:
            raise RoundTripMismatch(
                f"section {section}: the rebuilt record lines have sha256 {sec.hexdigest()}, the build "
                f"recorded {want}"
            )
        sink.write(b"  }" + (b",\n" if i < len(sections) - 1 else b"\n"))
    sink.write(b"}\n")


def _record_lines(con: duckdb.DuckDBPyConnection, t: Table):
    """The table's record lines (indented, no comma) in ordinal order, in batches. A failing row (a pointer
    section outside the column's targets) stops it."""
    order = _ident(t.columns_of("ordinal")[0].name)
    try:
        rel = con.sql(f"SELECT '    ' || ({line_sql(t)}) FROM {_ident(t.name)} ORDER BY {order}")
        while True:
            batch = rel.fetchmany(_FETCH)
            if not batch:
                return
            lines = [r[0] for r in batch]
            if None in lines:
                raise RoundTripError(f"table {t.name}: a row wrote no line (a NULL key?)")
            yield lines
    except duckdb.Error as e:
        raise RoundTripError(f"table {t.name}: {e}") from e


def verify_roundtrip(
    out_dir: Path,
    *,
    full: str | None = None,
    sha256: str | None = None,
    spec_arg: str | None = None,
    memory_limit: str | None = None,
    threads: int | None = None,
    work_dir: Path | None = None,
) -> dict:
    """`verify --roundtrip`: rebuild the text without writing it and compare it with the recorded sha256s (a
    mismatch is a difference, not an error); with `full`, also with that bundle's uncompressed text (its
    compressed sha256 must be the files' source_bundle_sha256). Refusals (not a lossless build) raise."""
    from . import bundle, sources
    from .verify import dir_kv

    out: dict = {"passed": False, "problems": [], "checked": [], "text_sha256": None, "text_size": None}
    try:
        r = rebuild(
            out_dir, None, spec_arg=spec_arg, memory_limit=memory_limit, threads=threads, work_dir=work_dir
        )
    except RoundTripMismatch as e:
        out["problems"].append(str(e))
        return out
    out.update(text_sha256=r.text_sha256, text_size=r.text_size, sections=r.sections)
    out["checked"].append("every section's section_text_sha256")
    out["checked"].append("the manifest's source_text_sha256")
    if full is not None:
        con = duckdb.connect()
        try:
            recorded = _shared_kv(dir_kv(con, Path(out_dir)), "source_bundle_sha256")
        finally:
            con.close()
        inp = sources.resolve_input(full, sha256=sha256)
        streamed = bundle.stream_bundle(inp, _TextSink(), text_digests=True)
        if streamed.sha256 != recorded:
            raise RoundTripError(
                f"--full {full} has sha256 {streamed.sha256}, but the files were built from {recorded}"
            )
        if streamed.text_sha256 != r.text_sha256:
            out["problems"].append(
                f"the rebuilt text has sha256 {r.text_sha256}, --full {inp.name}'s text {streamed.text_sha256}"
            )
            return out
        out["checked"].append(f"the text of {inp.name}")
    out["passed"] = True
    return out


# --- CLI -------------------------------------------------------------------------------------------------------


def run_rebuild_command(args) -> int:
    """`clinvar-gkm-parquet rebuild`: write the bundle back; prints its sha256."""
    result = rebuild(
        args.dir,
        args.out,
        spec_arg=args.spec,
        memory_limit=args.memory_limit,
        threads=args.threads,
        work_dir=args.work_dir,
        log=lambda m: print(f"clinvar-gkm-parquet: {m}", file=sys.stderr, flush=True),
    )
    total = sum(result.sections.values())
    print(
        f"rebuilt {result.out}: bundle schema {result.schema_version}, {len(result.sections)} section(s), {total} "
        f"records, {result.text_size} bytes of text, sha256 {result.text_sha256} (matches the manifest's "
        "source_text_sha256 and every section's section_text_sha256)"
    )
    return 0
