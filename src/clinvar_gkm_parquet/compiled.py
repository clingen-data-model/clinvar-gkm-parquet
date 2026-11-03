"""A compiled spec, as the tool's stages use it: compiled from a pairing, checked, written into a built directory,
and read back from one (ADR 0006).

A pairing is a bundle schema and its column set, each a path or URL. With neither given, the default bundle's
pairing for the bundle's version is used (default_pairing.py). This is the version gate of that default: a version
is accepted when its schema set and column set load and compile, and refused with the reason otherwise.

A build writes the spec it compiled as COMPILED_SPEC_FILE beside its manifest, so `verify` and `rebuild` need no
schema: they load that file and hold its hash against the one every file records.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path

import yaml

from . import default_pairing
from .bundle import BundleShape, RootRules
from .columnset import load_column_set
from .compiler import compile_sections, spec_for_sorted_input, table_spec_sha256
from .errors import CompileError, SchemaSetError, SchemaVersionError, SpecError
from .schemas import load_schema_set
from .spec import Spec, SpecSource, check_compiled, parse_spec

COMPILED_SPEC_FILE = "table-spec.json"


def _from_raw(raw: dict, schema_set_sha256: str, column_set_sha256: str) -> tuple[Spec, SpecSource]:
    where = f"sections compiled from schema set {schema_set_sha256}"
    spec = parse_spec(raw, where=where, require_name=True)
    check_compiled(spec, where)
    source = SpecSource(
        name=spec.name,
        version=spec.spec_version,
        builtin=True,
        path=None,
        sha256=table_spec_sha256(raw),
        schema_set_sha256=schema_set_sha256,
        column_set_sha256=column_set_sha256,
        raw=raw,
    )
    return spec, source


@cache
def compile_pairing(schema_source: str, column_set_source: str) -> tuple[Spec, SpecSource]:
    """The spec of a pairing, checked, and its source. Raises SchemaSetError, CompileError or SpecError."""
    schema_set = load_schema_set(schema_source)
    column_set = load_column_set(column_set_source)
    raw = compile_sections(schema_set, column_set.doc)
    return _from_raw(raw, schema_set.sha256, column_set.sha256)


def compiled_spec(version: str) -> tuple[Spec, SpecSource]:
    """The spec of the default bundle at `version`; refused with the reason if it doesn't load and compile."""
    try:
        return compile_pairing(default_pairing.schema_source(version), default_pairing.column_set_source(version))
    except (SchemaSetError, CompileError, SpecError) as e:
        raise SchemaVersionError(
            f"bundle schema_version {version!r}: table spec sections can't read it: {e}"
        ) from e


def for_sorted_input(spec: Spec, source: SpecSource) -> tuple[Spec, SpecSource]:
    """The spec for a bundle the general reader normalizes (compiler.spec_for_sorted_input)."""
    return _from_raw(spec_for_sorted_input(source.raw), source.schema_set_sha256, source.column_set_sha256)


def reader_rules(spec: Spec) -> RootRules:
    """How to read a bundle's root for `spec`: a compiled spec says; any other reads the default bundle's."""
    if spec.root is None:
        return default_pairing.reader_rules()
    shape = BundleShape(tuple(s for t in spec.tables for s in t.sections), spec.root.other_values)
    return RootRules(spec.root.version_property, lambda version: shape)


def write_compiled(source: SpecSource, out_dir: Path) -> Path:
    path = Path(out_dir) / COMPILED_SPEC_FILE
    path.write_text(json.dumps(source.raw, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def is_recorded(kv: dict[str, str]) -> bool:
    """A file's key-value metadata (keys without the prefix) says a compiled spec wrote it."""
    return "schema_set_sha256" in kv


def recorded_spec(out_dir: Path, kv: dict[str, str]) -> Spec:
    """The compiled spec that wrote a set, from the file beside its manifest, checked against the hash the files
    record. A difference is refused, saying which hash and which tool version wrote the set."""
    path = Path(out_dir) / COMPILED_SPEC_FILE
    tool = kv.get("tool_version", "?")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as e:
        raise SpecError(
            f"{path}: the files name a compiled spec (written by tool {tool}), and it is missing: {e}"
        ) from e
    except ValueError as e:
        raise SpecError(f"{path}: not a compiled spec: {e}") from e
    want = kv.get("table_spec_sha256")
    got = table_spec_sha256(raw) if isinstance(raw, dict) and "tables" in raw else None
    if got != want:
        raise SpecError(
            f"{path}: the compiled spec differs from what the files record: its table_spec_sha256 is {got}, the "
            f"files (written by tool {tool}) record {want}"
        )
    spec = parse_spec(raw, where=str(path), require_name=True)
    check_compiled(spec, str(path))
    return spec


def compiled_yaml(source: SpecSource) -> str:
    """A compiled spec as YAML (`compile-spec`), with its hashes in a comment."""
    header = (
        f"# Table spec {source.name} {source.version}, compiled (ADR 0006).\n"
        f"# schema_set_sha256: {source.schema_set_sha256}\n"
        f"# column_set_sha256: {source.column_set_sha256}\n"
        f"# table_spec_sha256: {source.sha256} (of what affects the tables, compiler.table_spec_sha256)\n"
    )
    return header + yaml.safe_dump(source.raw, sort_keys=False, allow_unicode=True, width=100_000)
