"""Load a column set: the file a producer publishes with a bundle schema, stating the tables built from its
bundles.

The format is in docs/column-set-format.md; compiler.py holds its meaning. This module reads the file (a local path
or URL, YAML; a URL is cached), refuses an entry written twice, and checks the top-level keys.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml

from .errors import CompileError
from .schemas import default_cache_dir
from .transport import is_url, read_all

TOP_KEYS = frozenset({"schema", "version_property", "collections", "key_order", "sections"})


@dataclass(frozen=True)
class ColumnSet:
    source: str
    sha256: str  # of the file's bytes
    doc: dict


class _Loader(yaml.SafeLoader):
    """YAML's last-one-wins would silently hide an entry written twice."""

    def construct_mapping(self, node, deep=False):
        keys = [k.value for k, _ in node.value if k.tag != "tag:yaml.org,2002:merge"]
        twice = sorted({k for k in keys if keys.count(k) > 1})
        if twice:
            raise yaml.constructor.ConstructorError(None, None, f"{twice} written twice", node.start_mark)
        return super().construct_mapping(node, deep=deep)


def parse_column_set(raw: bytes, source: str) -> ColumnSet:
    try:
        doc = yaml.load(raw.decode("utf-8"), Loader=_Loader)
    except (UnicodeDecodeError, yaml.YAMLError) as e:
        raise CompileError(f"column set {source}: can't read it: {e}") from e
    check_keys(doc, source)
    return ColumnSet(source, hashlib.sha256(raw).hexdigest(), doc)


def check_keys(doc: object, source: str) -> None:
    if not isinstance(doc, dict):
        raise CompileError(f"column set {source}: not a mapping")
    unknown = sorted(set(doc) - TOP_KEYS)
    if unknown:
        raise CompileError(f"column set {source}: unknown key(s) {unknown} (it may hold {sorted(TOP_KEYS)})")
    if not isinstance(doc.get("schema"), str) or not doc["schema"]:
        raise CompileError(f"column set {source}: `schema` must name the $id of the schema it pairs with")
    if not isinstance(doc.get("sections"), dict) or not doc["sections"]:
        raise CompileError(f"column set {source}: `sections` must map each collection to its properties")


def load_column_set(source: str | Path, *, cache_dir: Path | None = None) -> ColumnSet:
    """The column set at `source`, a file or URL. A URL is fetched once and cached."""
    source = str(source)
    if not is_url(source):
        try:
            return parse_column_set(Path(source).read_bytes(), source)
        except OSError as e:
            raise CompileError(f"column set {source}: {e.strerror or e}") from e
    cached = (cache_dir or default_cache_dir()) / "column-sets" / hashlib.sha256(source.encode()).hexdigest()
    if cached.is_file():
        return parse_column_set(cached.read_bytes(), source)
    try:
        raw = read_all(source)
    except Exception as e:
        raise CompileError(f"column set {source}: can't fetch it: {e}") from e
    column_set = parse_column_set(raw, source)
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(raw)
    return column_set
