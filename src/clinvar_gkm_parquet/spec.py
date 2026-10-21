"""Load and check the table spec (`spec/<MAJOR>.<MINOR>/tables.yaml`, design plan §3.1).

The spec is the contract: every table the tool writes, and per column its name, exact DuckDB type,
empty-versus-NULL rule and derivation from the bundle. The loader is strict: an unknown key, a missing key or a
value outside the allowed set raises SpecError, so a typo in the spec can't change the output silently.

Format (YAML):

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
        description: <one line>
        columns:
          - name: id
            type: VARCHAR                 # exact DuckDB type string, as DESCRIBE prints it
            nulls: never                  # never | when-absent | empty-list
            derivation:
              kind: key                   # see DERIVATION_KINDS
              path: null                  # bundle JSON path ($.location), or null for key / record
              transform: null             # free text for computed columns, or null
            description: <one line>
        extension_slots:                  # only for tables with extension-slot columns
          extensions:                     # column name
            clinvarScvId: value_string    # extension name -> the slot field that holds its value

`nulls`: `never` = the value is always present; `when-absent` = NULL when the bundle omits the field;
`empty-list` = a list column that is `[]`, never NULL, when the bundle omits it (BigQuery's rule, §3.5 item 3).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import yaml

from .errors import SpecError

STATUSES = ("draft", "captured", "released")
NULL_RULES = ("never", "when-absent", "empty-list")
# Design plan §3.4.
DERIVATION_KINDS = (
    "key",  # the bundle key of the record
    "record",  # the bundle record, byte for byte as on its bundle line
    "scalar-path",  # one JSON path, cast to the column type
    "bare-id",  # a #/<section>/<id> pointer with ^#/[^/]+/ removed (also lists of pointers)
    "pointer-section",  # the <section> part of a pointer
    "typed-copy",  # the record field cast to the column's nested type, struct fields by name
    "extension-slot",  # {name, value} -> {name, value_<slot>: value} via extension_slots
    "computed",  # a documented function of record fields (transform says which)
)
_TABLE_KEYS = {"name", "sections", "row_filter", "sort_key", "description", "columns", "extension_slots"}
_COLUMN_KEYS = {"name", "type", "nulls", "derivation", "description"}
_DERIVATION_KEYS = {"kind", "path", "transform"}
_TOP_KEYS = {"spec_version", "status", "bundle_schema_versions", "sources", "writer", "tables"}


@dataclass(frozen=True)
class Derivation:
    kind: str
    path: str | None
    transform: str | None


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nulls: str
    derivation: Derivation
    description: str


@dataclass(frozen=True)
class Table:
    name: str
    sections: tuple[str, ...]
    row_filter: str | None
    sort_key: str
    description: str
    columns: tuple[Column, ...]
    extension_slots: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def file_name(self) -> str:
        return f"{self.name}.parquet"

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise SpecError(f"table {self.name}: no column {name!r}")

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

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise SpecError(f"spec {self.spec_version}: no table {name!r}")

    @property
    def sections(self) -> set[str]:
        return {s for t in self.tables for s in t.sections}


def spec_dir(version: str) -> Path:
    """The spec directory: packaged in the wheel as clinvar_gkm_parquet/_spec, or spec/ in a source tree."""
    packaged = Path(__file__).parent / "_spec" / version
    if packaged.is_dir():
        return packaged
    source_tree = Path(__file__).resolve().parents[2] / "spec" / version
    if source_tree.is_dir():
        return source_tree
    raise SpecError(f"table spec {version} not found (looked in {packaged} and {source_tree})")


@cache
def load_spec(version: str) -> Spec:
    path = spec_dir(version) / "tables.yaml"
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return parse_spec(raw, where=str(path))


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


def parse_spec(raw: object, where: str = "tables.yaml") -> Spec:
    top = _keys(raw, _TOP_KEYS, _TOP_KEYS, where)
    if top["status"] not in STATUSES:
        raise SpecError(f"{where}: status must be one of {STATUSES}")
    if not re.fullmatch(r"\d+\.\d+", str(top["spec_version"])):
        raise SpecError(f"{where}: spec_version must be MAJOR.MINOR")
    tables = []
    names: set[str] = set()
    for i, t in enumerate(top["tables"] or []):
        tw = f"{where}: tables[{i}]"
        t = _keys(t, _TABLE_KEYS, _TABLE_KEYS - {"extension_slots"}, tw)
        tw = f"{where}: table {t['name']}"
        if t["name"] in names:
            raise SpecError(f"{tw}: duplicate table")
        names.add(t["name"])
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
            d = _keys(c["derivation"], _DERIVATION_KEYS, _DERIVATION_KEYS, f"{cw}: derivation")
            if d["kind"] not in DERIVATION_KINDS:
                raise SpecError(f"{cw}: derivation kind must be one of {DERIVATION_KINDS}")
            columns.append(
                Column(
                    name=c["name"],
                    type=str(c["type"]),
                    nulls=c["nulls"],
                    derivation=Derivation(kind=d["kind"], path=d["path"], transform=d["transform"]),
                    description=str(c["description"]),
                )
            )
        if t["sort_key"] not in col_names:
            raise SpecError(f"{tw}: sort_key {t['sort_key']!r} is not a column")
        slots = t.get("extension_slots") or {}
        for col, mapping in slots.items():
            if col not in col_names:
                raise SpecError(f"{tw}: extension_slots names unknown column {col!r}")
            if not isinstance(mapping, dict) or not mapping:
                raise SpecError(f"{tw}: extension_slots.{col} must be a non-empty mapping")
        for c in columns:
            if c.derivation.kind == "extension-slot" and c.name not in slots:
                raise SpecError(f"{tw}: column {c.name} is extension-slot but has no extension_slots entry")
        tables.append(
            Table(
                name=t["name"],
                sections=tuple(t["sections"]),
                row_filter=t["row_filter"],
                sort_key=t["sort_key"],
                description=str(t["description"]),
                columns=tuple(columns),
                extension_slots={k: dict(v) for k, v in slots.items()},
            )
        )
    return Spec(
        spec_version=str(top["spec_version"]),
        status=top["status"],
        bundle_schema_versions=tuple(top["bundle_schema_versions"]),
        sources=tuple(top["sources"] or ()),
        writer=dict(top["writer"]),
        tables=tuple(tables),
    )
