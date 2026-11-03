"""Load and check table specs: the built-in ones and a user's own tables.yaml (design plan §3.1, ADR 0003).

A spec is the contract: every table the tool writes, and per column its name, exact DuckDB type,
empty-versus-NULL rule and derivation from the bundle. The loader is strict: an unknown key, a missing key or a
value outside the allowed set raises SpecError, so a typo in the spec can't change the output silently.

Format (YAML):

    name: clinvar-gkm                     # the spec's name: lower case, digits and single hyphens
    spec_version: "1.0"
    status: draft | captured | released
    bundle_schema_versions: ["1.1.0"]
    sources: [<free text: where names and types came from, with refs>]
    writer: {compression: snappy, row_group_size: 122880}
    tables:
      - name: allele                      # output file is <name>.parquet
        sections: [allele]                # bundle section(s) the rows come from
        row_filter: null                  # null, or a description of the row rule (e.g. the evidence-line split)
        sort_key: id                      # the output column the file is ordered by
        sort_key_unique: <text>           # optional: why the sort column is unique, when it isn't the key
        known_fields: true                # optional (default false): see below
        description: <one line>
        columns:
          - name: id
            type: VARCHAR                 # exact DuckDB type string, as DESCRIBE prints it
            nulls: never                  # never | when-absent | empty-list
            derivation:
              kind: key                   # see DERIVATION_KINDS
              path: null                  # bundle JSON path ($.location), $ for refs, null for key / record
              transform: null             # free text for computed columns (and notes), or null
              pointers: ["[*].allele"]    # optional, typed-copy / extension-slot only: see below
              targets: [location]         # optional, bare-id / pointer-section only: see below
            description: <one line>
        extension_slots:                  # tables with extension-slot columns (or typed copies holding extensions)
          extensions:                     # column name
            clinvarScvId: value_string    # extension name -> the slot field that holds its value
          classification: json-type       # or: each extension fills the slot of its value's JSON type (below)
        json:                             # optional: how a row is written back as its record (below)
          key_order: columns              # columns | sorted
          record_id: true                 # the record has its own id, equal to the key
          sorted_columns: [extensions]    # optional (key_order columns): columns whose values are key-sorted
    lossless: true                        # optional (default false): the spec encodes the bundle text (below)

Extension slots: an extension struct is `STRUCT(name VARCHAR, <other fields>, value_<slot> <type>, ...)`; the
bundle's `{name, value, ...}` fills one slot, all others NULL (`[]` for a list slot). Which slot, per column of
`extension_slots`, at every depth of the column type:

- a mapping `{<extension name>: value_<slot>}`: the slot the extension's name maps to; a name the mapping doesn't
  list for that struct stops the build.
- `json-type`: the slot of the value's JSON type, whatever the name: string -> value_string, boolean ->
  value_boolean, integer -> value_integer, any other number -> value_number, object -> value_object, array ->
  value_<k>_array where <k> is the JSON type of its elements (value_string_array, value_object_array, ...). An
  empty array fills no slot (the struct must have an array slot). A JSON null or absent value, an array whose
  elements are of more than one JSON type, or a type the struct has no slot for stops the build.

`nulls`: `never` = the value is always present; `when-absent` = NULL when the bundle omits the field;
`empty-list` = a list column that is `[]`, never NULL, when the bundle omits it (BigQuery's rule, §3.5 item 3).

Paths (`derivation.path`; checked when the spec loads, `path_problems`). Deliberately small, not JSONPath:

- `$.a.b` reads field `b` of field `a` of the record. Field names are letters, digits and `_`.
- `[*]` after a field means each element of that list: `$.contributions[*].contributor` is the contributor of
  every contribution, in order. A path may hold several (`$.a[*].b[*]`); each one is one list level of the
  column type, and the result nests (no flattening).
- The column type decides the rest: a list type one level deeper than the path's `[*]`s reads the value at the
  end of the path as a list, so `$.members` and `$.members[*]` mean the same for a `VARCHAR[]` column.
- Where each kind may use what: `key` and `record` take no path (null); `refs` takes `$` (the whole record);
  `scalar-path`, `bare-id` and `pointer-section` take any path, `[*]` included; `typed-copy`, `extension-slot`
  and the range kinds take fields only (`$.a.b`): the column type describes the lists below them; `computed`
  takes any path, or null (its SQL file says what it reads).
- Reading: the fields before the first `[*]` read like DuckDB's json_extract (a field of an absent, null or
  non-object value is absent). From the first `[*]` on, every step is checked: a `[*]` on a value that is not
  a list, or a field step on a value that is not an object, stops the build (absent and null are absent). An
  absent list is `[]` inside the column value, and follows `nulls` at the column's top level.
- At the leaf, `scalar-path` needs a scalar (an object or array stops the build) and casts it to the type;
  `bare-id` and `pointer-section` need a `#/<section>/<id>` string (anything else stops the build).
- `range-exact`, `range-min`, `range-max` (BIGINT) read a VRS integer-or-Range: an integer, or a Range
  `[min, max]` whose ends are integers or null. range-exact is the integer, NULL for a Range; range-min and
  range-max are the integer, else element [0] or [1] of the Range. Any other value stops the build.

`pointers` (typed-copy and extension-slot only): the positions inside the column value that hold
`#/<section>/<id>` pointers, written in the column type's field names, relative to the value: `[*]` each list
element, `.f` a struct field (`[*].allele`, `[*].value_gene_array[*].gene`). Each position must be a VARCHAR
of the type; the copy holds the bare id there, and a non-pointer stops the build.

`targets` (bare-id and pointer-section): the bundle section(s) the pointer may name. In `pointers`, a mapping
`{<position>: [<section>, ...]}` gives the same per position (a plain list of positions gives none). A table with
a `json` block has its pointers checked against their targets (the build's round-trip check, below).

`ordinal` (derivation kind; BIGINT, nulls never, no path): the record's position in its bundle section, 0 for the
section's first record line, in the order the build staged the section (the full's order, then any replayed
records). It is derived from the bundle (ADR 0001).

`json` (a table's JSON layout, ADR 0005): how a row is written back as its bundle record, byte for byte. The key
column is the record's `id` property when `record_id` (the record has none otherwise); each other column is the
top-level property its path names, except that the range columns of one path (range-exact, range-min,
range-max) are one integer-or-Range property, and pointer-section and ordinal columns are no property (a
pointer-section column gives the section of the multi-target bare-id column of the same path). `key_order:
columns` writes the properties in column order and every object in its STRUCT field order, an extension's
`value_*` slots as one `value` at the first slot's place; `sorted` sorts every key of the record by its UTF-8
bytes, at every depth (BigQuery's JSON order); `sorted_columns` sorts the values of those columns only. A `json`
block also changes the build: an absent list inside a typed value is NULL, not `[]` (so NULL is absent and `[]`
empty at every depth, as at the top level), and every row is written back as JSON and compared with its bundle
line (key and record, byte for byte); any difference stops the build.

`lossless: true`: the spec is a lossless encoding of the bundle's uncompressed text (ADR 0005), which
`clinvar-gkm-parquet rebuild` writes back. Every table needs a `json` block and an `ordinal` column as its last
column; each section of each bundle schema_version has exactly one table, with no row filter and with
`known_fields`; and the JSON layout must place every column (roundtrip.lossless_problems).

`known_fields: true`: every top-level field of a record must be read by some column at a path `$.<field>`
(BigQuery's `SELECT t.*` tables, design plan §3.5); a record with another top-level field stops the build. It
needs every column with a path to read a top-level field (`$.f`, optionally followed by `[*]`). The record's own
`id` field counts as read by the key column: when present (and not null) it must equal the bundle key, or the
build stops.

Renderers (`baseline.renderer_for`): a table of only key, record and refs columns is rendered by baseline.py; a
table that builds exactly what the clinvar-gkm table of its name builds, by that table's shipped
`sql/<table>.sql` (sqltemplate.render; `builtin_sql_applies`); any other table by the generic renderer
(sqltemplate.render_generic), which builds the SELECT from the derivation kinds alone. The generic renderer
can't build a `computed` column or a row filter: those need a shipped SQL file, so a custom table with one
fails when the spec loads.

Two layers of checks:

- `parse_spec` checks the format (keys, allowed values, columns that exist). It works on any mapping, so tests
  can build small specs over made-up sections.
- `check_rules` checks what a spec must hold to be built (`load_spec_file` runs it on every spec file, built-in
  or custom): a name; only accepted bundle schema_versions and their sections; one key column and at most one
  record column per table (build.py's self-checks compare them with the bundle's key and record; a table with
  no record column is checked on its keys); a sort key that is the key column, or a documented unique one;
  writer settings build.py can use; a renderer for every table.

Built-in specs are named `sections` (the default), `baseline` and `clinvar-gkm`, each with the one version
compat.py lists; `resolve_spec` takes a built-in name (`clinvar-gkm`, `clinvar-gkm-1.0`, `clinvar-gkm@1.0`) or a
path.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from functools import cache
from importlib import resources
from pathlib import Path

import yaml

from .compat import ACCEPTED_SCHEMA_VERSIONS, BUILTIN_SPECS, DEFAULT_SPEC, PUBLISHED_SPEC, SCHEMA_SECTIONS
from .errors import SpecError

STATUSES = ("draft", "captured", "released")
NULL_RULES = ("never", "when-absent", "empty-list")
# Design plan §3.4, plus refs (ADR 0003).
DERIVATION_KINDS = (
    "key",  # the bundle key of the record
    "record",  # the bundle record, byte for byte as on its bundle line
    "scalar-path",  # one JSON path, cast to the column type
    "bare-id",  # a #/<section>/<id> pointer with ^#/[^/]+/ removed (also lists of pointers)
    "pointer-section",  # the <section> part of a pointer
    "typed-copy",  # the record field cast to the column's nested type, struct fields by name
    "extension-slot",  # {name, value} -> {name, value_<slot>: value} via extension_slots
    "computed",  # a documented function of record fields (transform says which)
    "refs",  # every #/<section>/<id> pointer string in the record, with its path, in document order (baseline.py)
    "range-exact",  # a VRS integer-or-Range: the integer; NULL for a Range [min, max]
    "range-min",  # the integer, else the Range's [0]
    "range-max",  # the integer, else the Range's [1]
    "ordinal",  # the record's position in its bundle section (0-based)
)
RANGE_KINDS = ("range-exact", "range-min", "range-max")
JSON_TYPE_SLOTS = "json-type"  # extension_slots.<column>: each extension fills the slot of its value's JSON type
_TABLE_KEYS = {
    "name",
    "sections",
    "row_filter",
    "sort_key",
    "sort_key_unique",
    "description",
    "columns",
    "extension_slots",
    "known_fields",
    "json",
}
_TABLE_OPTIONAL = {"extension_slots", "sort_key_unique", "known_fields", "json"}
_COLUMN_KEYS = {"name", "type", "nulls", "derivation", "description"}
_DERIVATION_KEYS = {"kind", "path", "transform", "pointers", "targets"}
_DERIVATION_OPTIONAL = {"pointers", "targets"}
_JSON_KEYS = {"key_order", "record_id", "sorted_columns"}
KEY_ORDERS = ("columns", "sorted")
# Paths (module docstring). LIST_PATH: fields, each optionally followed by [*]; FIELD_PATH: fields only.
LIST_PATH_RE = re.compile(r"\$(?:\.\w+(?:\[\*\])*)+")
FIELD_PATH_RE = re.compile(r"\$(?:\.\w+)+")
POINTER_POS_RE = re.compile(r"(?:\[\*\]|\.\w+)+")
_POINTER_KINDS = ("typed-copy", "extension-slot")
_FIELD_PATH_KINDS = (*_POINTER_KINDS, *RANGE_KINDS)
_TOP_KEYS = {"name", "spec_version", "status", "bundle_schema_versions", "sources", "writer", "tables", "lossless"}
_TOP_OPTIONAL = {"name", "lossless"}
NAME_RE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*")
VERSION_RE = re.compile(r"\d+\.\d+")

# Where each built-in spec lives under spec/ (source tree) or clinvar_gkm_parquet/_spec/ (wheel).
# clinvar-gkm is still at spec/<version>/; when it moves to spec/clinvar-gkm/<version>/, change its entry to
# "clinvar-gkm/{version}".
BUILTIN_DIRS: dict[str, str] = {
    "sections": "sections/{version}",
    "baseline": "baseline/{version}",
    "clinvar-gkm": "{version}",
}
SPEC_FILE = "tables.yaml"


@dataclass(frozen=True)
class Derivation:
    kind: str
    path: str | None
    transform: str | None
    pointers: tuple[str, ...] = ()  # typed-copy / extension-slot: pointer positions in the value (docstring)
    targets: tuple[str, ...] = ()  # bare-id / pointer-section: the sections the pointer may name
    pointer_targets: tuple[tuple[str, ...], ...] = ()  # per position of `pointers` when given as a mapping


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nulls: str
    derivation: Derivation
    description: str


@dataclass(frozen=True)
class JsonLayout:
    """A table's `json` block: how a row is written back as its bundle record (module docstring)."""

    key_order: str  # columns | sorted
    record_id: bool
    sorted_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Table:
    name: str
    sections: tuple[str, ...]
    row_filter: str | None
    sort_key: str
    description: str
    columns: tuple[Column, ...]
    # column -> {extension name: slot}, or JSON_TYPE_SLOTS (docstring)
    extension_slots: dict[str, dict[str, str] | str] = field(default_factory=dict)
    sort_key_unique: str | None = None
    known_fields: bool = False  # every top-level record field must be read by a column (docstring)
    json: JsonLayout | None = None  # the record's JSON layout (round trip, ADR 0005)

    @property
    def file_name(self) -> str:
        return f"{self.name}.parquet"

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise SpecError(f"table {self.name}: no column {name!r}")

    def columns_of(self, kind: str) -> list[Column]:
        return [c for c in self.columns if c.derivation.kind == kind]

    def describe(self) -> list[tuple[str, str]]:
        """[(column_name, column_type)] in order: what DESCRIBE of the built file must equal."""
        return [(c.name, c.type) for c in self.columns]


@dataclass(frozen=True)
class Spec:
    spec_version: str
    status: str
    bundle_schema_versions: tuple[str, ...]
    sources: tuple[str, ...]
    writer: dict
    tables: tuple[Table, ...]
    name: str | None = None  # required in every spec file; may be absent only in a mapping given to parse_spec
    lossless: bool = False  # the spec encodes the bundle text: `rebuild` writes it back (ADR 0005)

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise SpecError(f"spec {self.label}: no table {name!r}")

    @property
    def label(self) -> str:
        return f"{self.name} {self.spec_version}" if self.name else self.spec_version

    @property
    def sections(self) -> set[str]:
        return {s for t in self.tables for s in t.sections}


@dataclass(frozen=True)
class SpecSource:
    """Where a resolved spec came from: what the build labels its output with (`kv_metadata`)."""

    name: str
    version: str
    builtin: bool
    path: Path
    sha256: str  # of the tables.yaml bytes

    @property
    def label(self) -> str:
        if self.builtin:
            return f"{self.name} {self.version}"
        return f"{self.name} {self.version} (custom, {self.path}, sha256 {self.sha256})"


# --- locating and loading --------------------------------------------------------------------------------------


def builtin_dir(name: str, version: str) -> Path:
    """A built-in spec's directory: in the wheel under clinvar_gkm_parquet/_spec, or spec/ in a source tree."""
    if BUILTIN_SPECS.get(name) != version:
        have = ", ".join(f"{n} {v}" for n, v in BUILTIN_SPECS.items())
        raise SpecError(f"no built-in table spec {name} {version} (this tool has {have})")
    rel = BUILTIN_DIRS[name].format(version=version)
    packaged = Path(__file__).parent / "_spec" / rel
    if packaged.is_dir():
        return packaged
    source_tree = Path(__file__).resolve().parents[2] / "spec" / rel
    if source_tree.is_dir():
        return source_tree
    raise SpecError(f"table spec {name} {version} not found (looked in {packaged} and {source_tree})")


def spec_dir(version: str, name: str = PUBLISHED_SPEC) -> Path:
    """The directory of built-in spec `name` (default clinvar-gkm, the only spec before specs had names)."""
    return builtin_dir(name, version)


def load_spec(version: str, name: str = PUBLISHED_SPEC) -> Spec:
    """Built-in spec `name` at `version` (default clinvar-gkm: `load_spec("1.0")` is the 22-table spec)."""
    return _load_builtin(name, version)


@cache
def _load_builtin(name: str, version: str) -> Spec:
    return load_spec_file(builtin_dir(name, version) / SPEC_FILE, builtin=True)[0]


def load_builtin(name: str, version: str | None = None) -> Spec:
    return load_spec(version or BUILTIN_SPECS[name], name)


def load_spec_file(path: Path, *, builtin: bool = False) -> tuple[Spec, str]:
    """(spec, sha256 of the file bytes). Format and rules are checked (`parse_spec`, `check_rules`)."""
    path = Path(path)
    try:
        data = path.read_bytes()
    except OSError as e:
        raise SpecError(f"can't read table spec {path}: {e}") from e
    try:
        raw = yaml.safe_load(data.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as e:
        raise SpecError(f"{path}: not a YAML file: {e}") from e
    spec = parse_spec(raw, where=str(path), require_name=True)
    check_rules(spec, where=str(path), builtin=builtin)
    return spec, hashlib.sha256(data).hexdigest()


_BUILTIN_ARG = re.compile(r"(?P<name>[a-z][a-z0-9]*(?:-[a-z][a-z0-9]*)*)(?:[-@](?P<version>\d+\.\d+))?")


def resolve_spec(arg: str | None) -> tuple[Spec, SpecSource]:
    """The spec a `--spec` value names. None: the default (sections). A built-in name, alone or with its version
    (`clinvar-gkm`, `clinvar-gkm-1.0`, `clinvar-gkm@1.0`); anything else is the path of a tables.yaml. A file
    that happens to share a built-in's name is reached with a path (`./baseline`)."""
    if arg is None:
        arg = DEFAULT_SPEC
    m = _BUILTIN_ARG.fullmatch(arg)
    if m and m["name"] in BUILTIN_SPECS:
        name = m["name"]
        version = m["version"] or BUILTIN_SPECS[name]
        path = builtin_dir(name, version) / SPEC_FILE
        spec = load_spec(version, name)
        if (spec.name, spec.spec_version) != (name, version):
            raise SpecError(f"{path}: says it is {spec.label}, but it is the built-in {name} {version}")
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        return spec, SpecSource(name=name, version=version, builtin=True, path=path, sha256=sha)
    path = Path(arg)
    if not path.is_file():
        raise SpecError(
            f"--spec {arg}: not a built-in table spec ({', '.join(BUILTIN_SPECS)}, optionally -<version> or "
            f"@<version>) and no file at {path}"
        )
    spec, sha = load_spec_file(path)
    return spec, SpecSource(name=spec.name, version=spec.spec_version, builtin=False, path=path, sha256=sha)


# --- key-value metadata (design plan §7.2, ADR 0003) -----------------------------------------------------------

# The spec keys build.py writes in every file's key-value metadata, without build.py's
# "clinvar_gkm_parquet." prefix, in this order. The last two are written for a custom spec only.
SPEC_KV_KEYS = ("table_spec", "table_spec_version", "table_spec_builtin", "table_spec_sha256")


def kv_metadata(source: SpecSource) -> dict[str, str]:
    """The spec's key-value metadata for every file of a build (keys without the prefix, in SPEC_KV_KEYS order).
    A built-in spec is named by name and version; a custom one also by `table_spec_builtin: false` and the
    sha256 of its file, since its name and version are only what the file says."""
    kv = {"table_spec": source.name, "table_spec_version": source.version}
    if not source.builtin:
        kv["table_spec_builtin"] = "false"
        kv["table_spec_sha256"] = source.sha256
    return kv


def spec_from_kv(kv: dict[str, str], spec_arg: str | None = None) -> tuple[Spec, SpecSource]:
    """The spec that wrote a file, from its key-value metadata (keys without the prefix). A file with no
    table_spec key predates named specs and was written by clinvar-gkm. A custom spec can't be found from the
    metadata: pass its path as `spec_arg`; its sha256 must equal the recorded one."""
    name = kv.get("table_spec", PUBLISHED_SPEC)
    version = kv.get("table_spec_version")
    if not version:
        raise SpecError("the file's key-value metadata has no table_spec_version")
    if kv.get("table_spec_builtin", "true") != "false":
        return resolve_spec(f"{name}@{version}")
    sha = kv.get("table_spec_sha256")
    if spec_arg is None:
        raise SpecError(
            f"the files were written by custom table spec {name} {version} (sha256 {sha}); "
            "pass that tables.yaml with --spec"
        )
    spec, source = resolve_spec(spec_arg)
    got = (source.name, source.version, source.builtin, source.sha256)
    want = (name, version, False, sha)
    if got != want:
        raise SpecError(
            f"--spec {spec_arg} is {source.label}, but the files were written by custom table spec "
            f"{name} {version} (sha256 {sha})"
        )
    return spec, source


# --- format ----------------------------------------------------------------------------------------------------


def path_problems(d: Derivation) -> list[str]:
    """What is wrong with a derivation's path and pointers for its kind (module docstring); empty: nothing. The
    checks that need the column type (list levels, leaf types, pointer positions) are the renderer's
    (sqltemplate.generic_problems)."""
    kind, path = d.kind, d.path
    out = []
    if kind in ("key", "record", "ordinal"):
        if path is not None:
            out.append(f"a {kind} column takes no path (null), not {path!r}")
    elif kind == "refs":
        if path != "$":
            out.append(f"a refs column takes path $ (the whole record), not {path!r}")
    elif kind in _FIELD_PATH_KINDS:
        if not (isinstance(path, str) and FIELD_PATH_RE.fullmatch(path)):
            out.append(f"a {kind} path is fields only ($.a.b), not {path!r}")
    elif path is not None or kind != "computed":
        if not (isinstance(path, str) and LIST_PATH_RE.fullmatch(path)):
            out.append(f"path {path!r} is not $.<field>, optionally with more .<field> and [*] steps")
    if d.pointers and kind not in _POINTER_KINDS:
        out.append(f"pointers is for {' and '.join(_POINTER_KINDS)} columns only")
    if d.targets and kind not in ("bare-id", "pointer-section"):
        out.append("targets is for bare-id and pointer-section columns only")
    for pos in d.pointers:
        if not POINTER_POS_RE.fullmatch(pos):
            out.append(f"pointer position {pos!r} is not [*] and .<field> steps (e.g. [*].allele)")
    if len(set(d.pointers)) != len(d.pointers):
        out.append("pointers lists a position twice")
    return out


def _keys(obj: object, allowed: set[str], required: set[str], where: str) -> dict:
    if not isinstance(obj, dict):
        raise SpecError(f"{where}: expected a mapping")
    unknown = set(obj) - allowed
    missing = required - set(obj)
    if unknown:
        raise SpecError(f"{where}: unknown key(s) {sorted(unknown)}")
    if missing:
        raise SpecError(f"{where}: missing key(s) {sorted(missing)}")
    return obj


def parse_spec(raw: object, where: str = "tables.yaml", *, require_name: bool = False) -> Spec:
    """Check the format and build the Spec. `name` is required when `require_name` (every spec file is loaded
    that way); a mapping without one is accepted for small in-memory specs."""
    top = _keys(raw, _TOP_KEYS, _TOP_KEYS - _TOP_OPTIONAL | ({"name"} if require_name else set()), where)
    name = top.get("name")
    if name is not None and not (isinstance(name, str) and NAME_RE.fullmatch(name)):
        raise SpecError(f"{where}: name {name!r} must be lower case letters and digits, joined by single hyphens")
    if top["status"] not in STATUSES:
        raise SpecError(f"{where}: status must be one of {STATUSES}")
    if not VERSION_RE.fullmatch(str(top["spec_version"])):
        raise SpecError(f"{where}: spec_version must be MAJOR.MINOR")
    if not isinstance(top["writer"], dict):
        raise SpecError(f"{where}: writer must be a mapping")
    lossless = top.get("lossless", False)
    if not isinstance(lossless, bool):
        raise SpecError(f"{where}: lossless must be true or false")
    tables = []
    names: set[str] = set()
    for i, t in enumerate(top["tables"] or []):
        tw = f"{where}: tables[{i}]"
        t = _keys(t, _TABLE_KEYS, _TABLE_KEYS - _TABLE_OPTIONAL, tw)
        tw = f"{where}: table {t['name']}"
        if t["name"] in names:
            raise SpecError(f"{tw}: duplicate table")
        names.add(t["name"])
        if not isinstance(t["sections"], list) or not t["sections"]:
            raise SpecError(f"{tw}: sections must be a non-empty list")
        columns = []
        col_names: set[str] = set()
        for j, c in enumerate(t["columns"] or []):
            cw = f"{tw}: columns[{j}]"
            c = _keys(c, _COLUMN_KEYS, _COLUMN_KEYS, cw)
            cw = f"{tw}: column {c['name']}"
            if c["name"] in col_names:
                raise SpecError(f"{cw}: duplicate column")
            col_names.add(c["name"])
            if c["nulls"] not in NULL_RULES:
                raise SpecError(f"{cw}: nulls must be one of {NULL_RULES}")
            d = _keys(
                c["derivation"], _DERIVATION_KEYS, _DERIVATION_KEYS - _DERIVATION_OPTIONAL, f"{cw}: derivation"
            )
            if d["kind"] not in DERIVATION_KINDS:
                raise SpecError(f"{cw}: derivation kind must be one of {DERIVATION_KINDS}")
            pointers = d.get("pointers", [])
            pointer_targets: tuple[tuple[str, ...], ...] = ()
            if isinstance(pointers, dict):
                if not all(_section_list(v) for v in pointers.values()):
                    raise SpecError(f"{cw}: derivation pointers: each position maps to a list of sections")
                pointer_targets = tuple(tuple(v) for v in pointers.values())
                pointers = list(pointers)
            if not (isinstance(pointers, list) and all(isinstance(x, str) for x in pointers)):
                raise SpecError(f"{cw}: derivation pointers must be a list of positions, or a mapping to sections")
            targets = d.get("targets", [])
            if "targets" in d and not _section_list(targets):
                raise SpecError(f"{cw}: derivation targets must be a non-empty list of section names")
            derivation = Derivation(
                kind=d["kind"],
                path=d["path"],
                transform=d["transform"],
                pointers=tuple(pointers),
                targets=tuple(targets),
                pointer_targets=pointer_targets,
            )
            bad = path_problems(derivation)
            if bad:
                raise SpecError(f"{cw}: {'; '.join(bad)}")
            columns.append(
                Column(
                    name=c["name"],
                    type=str(c["type"]),
                    nulls=c["nulls"],
                    derivation=derivation,
                    description=str(c["description"]),
                )
            )
        if t["sort_key"] not in col_names:
            raise SpecError(f"{tw}: sort_key {t['sort_key']!r} is not a column")
        unique_note = t.get("sort_key_unique")
        if unique_note is not None and not (isinstance(unique_note, str) and unique_note.strip()):
            raise SpecError(f"{tw}: sort_key_unique must be a non-empty description, or absent")
        slots = t.get("extension_slots") or {}
        for col, mapping in slots.items():
            if col not in col_names:
                raise SpecError(f"{tw}: extension_slots names unknown column {col!r}")
            if mapping != JSON_TYPE_SLOTS and (not isinstance(mapping, dict) or not mapping):
                raise SpecError(f"{tw}: extension_slots.{col} must be a non-empty mapping or {JSON_TYPE_SLOTS}")
        for c in columns:
            if c.derivation.kind == "extension-slot" and c.name not in slots:
                raise SpecError(f"{tw}: column {c.name} is extension-slot but has no extension_slots entry")
        known_fields = t.get("known_fields", False)
        if not isinstance(known_fields, bool):
            raise SpecError(f"{tw}: known_fields must be true or false")
        layout = _json_layout(t["json"], col_names, tw) if "json" in t else None
        tables.append(
            Table(
                name=t["name"],
                sections=tuple(t["sections"]),
                row_filter=t["row_filter"],
                sort_key=t["sort_key"],
                description=str(t["description"]),
                columns=tuple(columns),
                extension_slots={k: v if v == JSON_TYPE_SLOTS else dict(v) for k, v in slots.items()},
                sort_key_unique=unique_note,
                known_fields=known_fields,
                json=layout,
            )
        )
    return Spec(
        spec_version=str(top["spec_version"]),
        status=top["status"],
        bundle_schema_versions=tuple(top["bundle_schema_versions"]),
        sources=tuple(top["sources"] or ()),
        writer=dict(top["writer"]),
        tables=tuple(tables),
        name=name,
        lossless=lossless,
    )


def _section_list(v: object) -> bool:
    return isinstance(v, list) and bool(v) and all(isinstance(x, str) and x for x in v)


def _json_layout(raw: object, col_names: set[str], tw: str) -> JsonLayout:
    j = _keys(raw, _JSON_KEYS, {"key_order", "record_id"}, f"{tw}: json")
    if j["key_order"] not in KEY_ORDERS:
        raise SpecError(f"{tw}: json.key_order must be one of {KEY_ORDERS}")
    if not isinstance(j["record_id"], bool):
        raise SpecError(f"{tw}: json.record_id must be true or false")
    sorted_columns = j.get("sorted_columns", [])
    if not (isinstance(sorted_columns, list) and all(isinstance(c, str) for c in sorted_columns)):
        raise SpecError(f"{tw}: json.sorted_columns must be a list of column names")
    unknown = [c for c in sorted_columns if c not in col_names]
    if unknown:
        raise SpecError(f"{tw}: json.sorted_columns names unknown column(s) {unknown}")
    if sorted_columns and j["key_order"] == "sorted":
        raise SpecError(f"{tw}: json.sorted_columns is for key_order columns (sorted sorts every key)")
    return JsonLayout(key_order=j["key_order"], record_id=j["record_id"], sorted_columns=tuple(sorted_columns))


# --- rules -----------------------------------------------------------------------------------------------------


def _has_sql_template(table_name: str) -> bool:
    return resources.files("clinvar_gkm_parquet").joinpath("sql", f"{table_name}.sql").is_file()


def _table_shape(t: Table) -> tuple:
    """What a table builds, without its prose (descriptions, transform notes)."""
    cols = tuple(
        (c.name, c.type, c.nulls, c.derivation.kind, c.derivation.path, c.derivation.pointers) for c in t.columns
    )
    slots = tuple(
        sorted((k, v if v == JSON_TYPE_SLOTS else tuple(sorted(v.items()))) for k, v in t.extension_slots.items())
    )
    # (The json block changes what the build writes: absent nested lists, the round-trip check.)
    return (t.sections, t.row_filter, t.sort_key, t.known_fields, cols, slots, t.json)


def _same_as_builtin_table(t: Table) -> bool:
    """True if `t` builds exactly what the built-in clinvar-gkm table of the same name builds."""
    try:
        builtin = load_builtin(PUBLISHED_SPEC).table(t.name)
    except SpecError:
        return False
    return _table_shape(t) == _table_shape(builtin)


def builtin_sql_applies(t: Table) -> bool:
    """The tool's sql/<name>.sql renders `t`: it ships one, and `t` builds exactly what the clinvar-gkm table of
    that name builds (the file implements that table; a table of another spec with the same name, such as a
    sections table, takes the generic renderer)."""
    return _has_sql_template(t.name) and _same_as_builtin_table(t)


def check_rules(spec: Spec, where: str = "tables.yaml", *, builtin: bool = False) -> None:
    """What a spec must hold to be built (module docstring). Raises SpecError naming every problem found."""
    from .baseline import problems as baseline_problems  # baseline and sqltemplate import this module
    from .sqltemplate import generic_problems

    errs: list[str] = []
    spec_secs = {s for v in spec.bundle_schema_versions for s in SCHEMA_SECTIONS.get(v, ())}
    if spec.name is None:
        errs.append("no name")
    elif not builtin and spec.name in BUILTIN_SPECS:
        errs.append(f"name {spec.name!r} is a built-in spec's; a custom spec needs its own name")
    if not spec.bundle_schema_versions:
        errs.append("bundle_schema_versions is empty")
    unknown_v = [v for v in spec.bundle_schema_versions if v not in ACCEPTED_SCHEMA_VERSIONS]
    if unknown_v:
        errs.append(
            f"bundle_schema_versions {unknown_v} are not read by this tool "
            f"(it accepts {', '.join(ACCEPTED_SCHEMA_VERSIONS)})"
        )
    if not spec.tables:
        errs.append("no tables")
    compression = spec.writer.get("compression")
    if not (isinstance(compression, str) and re.fullmatch(r"[a-z0-9_]+", compression)):
        errs.append(f"writer.compression {compression!r} is not a codec name")
    if not isinstance(spec.writer.get("row_group_size"), int) or spec.writer["row_group_size"] <= 0:
        errs.append("writer.row_group_size must be a positive integer")
    unknown_w = set(spec.writer) - {"compression", "row_group_size"}
    if unknown_w:
        errs.append(f"writer: unknown key(s) {sorted(unknown_w)}")
    for t in spec.tables:
        tw = f"table {t.name}"
        for v in spec.bundle_schema_versions:
            if v in SCHEMA_SECTIONS:
                bad = [s for s in t.sections if s not in SCHEMA_SECTIONS[v]]
                if bad:
                    errs.append(f"{tw}: section(s) {bad} are not sections of bundle schema {v}")
        keys, records = t.columns_of("key"), t.columns_of("record")
        if len(keys) != 1:
            errs.append(f"{tw}: needs exactly one key column (derivation kind key), has {len(keys)}")
        if len(records) > 1:
            errs.append(f"{tw}: has {len(records)} record columns (derivation kind record); at most one")
        if keys and keys[0].nulls != "never":
            errs.append(f"{tw}: key column {keys[0].name} must have nulls: never")
        if not (keys and t.sort_key == keys[0].name):
            if t.sort_key_unique is None:
                errs.append(
                    f"{tw}: sort_key {t.sort_key} is not the key column; give sort_key_unique (why it is unique "
                    "and never NULL; the build still checks it)"
                )
            elif t.column(t.sort_key).nulls != "never":
                errs.append(f"{tw}: sort_key {t.sort_key} must have nulls: never")
        if t.row_filter is not None and not (isinstance(t.row_filter, str) and t.row_filter.strip()):
            errs.append(f"{tw}: row_filter must be null or a description")
        # The shipped sql/<name>.sql implements the built-in clinvar-gkm table of that name. While clinvar-gkm
        # itself loads, its tables are those (comparing with it would load it again).
        template = _has_sql_template(t.name) and (
            (builtin and spec.name == PUBLISHED_SPEC) or _same_as_builtin_table(t)
        )
        if baseline_problems(t) and _has_sql_template(t.name) and not builtin and not template:
            # A custom table may reuse a clinvar-gkm name only as defined there (a differing custom table of that
            # name is refused rather than silently given the generic renderer).
            errs.append(
                f"{tw}: the name {t.name!r} is a built-in clinvar-gkm table with its own SQL, and this table "
                "differs from it; reuse it exactly as clinvar-gkm defines it, or give the custom table another "
                "name"
            )
        elif baseline_problems(t) and not template:
            generic = generic_problems(t)
            if generic:
                errs.append(
                    f"{tw}: no renderer: the tool has no sql/{t.name}.sql, and the generic renderer can't build "
                    f"it: {'; '.join(generic)}"
                )
    for t in spec.tables:
        for c in t.columns:
            named = {s for ts in (c.derivation.targets, *c.derivation.pointer_targets) for s in ts}
            bad = sorted(named - spec_secs)
            if bad:
                errs.append(f"table {t.name}: column {c.name}: targets {bad} are not bundle sections")
    if spec.lossless or any(t.json for t in spec.tables):
        from .roundtrip import lossless_problems  # roundtrip imports this module

        errs.extend(lossless_problems(spec))
    if errs:
        raise SpecError(f"{where}: table spec {spec.label}: " + "; ".join(errs))
