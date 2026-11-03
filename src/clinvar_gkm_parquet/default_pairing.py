"""The tool's default bundle, as data: spec/sections/default-bundle.yaml says which root property holds the version
and where a version's schema and column set are published. A shipped snapshot of a version is used first.
Nothing else in the tool's compile-and-read path knows the default (ADR 0006).
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import yaml

from .bundle import BundleShape, RootRules
from .compiler import root_parts
from .errors import SchemaSetError, SchemaVersionError
from .schemas import MANIFEST, load_schema_set

_CONFIG = "sections/default-bundle.yaml"
_COLUMN_SET = "sections/column-sets/{version}.columns.yaml"


def _roots(name: str) -> tuple[Path, Path]:
    """In a wheel, and in a source tree."""
    return Path(__file__).parent / f"_{name}", Path(__file__).resolve().parents[2] / name


@cache
def config() -> dict:
    for root in _roots("spec"):
        path = root / _CONFIG
        if path.is_file():
            return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise SchemaSetError(f"no {_CONFIG} in this installation")


def version_property() -> str:
    return config()["version_property"]


def snapshot_versions() -> list[str]:
    """The versions this tool ships a schema set of, sorted by version number."""
    found = {p.parent.name for root in _roots("schemas") if root.is_dir() for p in root.glob(f"*/{MANIFEST}")}
    return sorted(found, key=lambda v: [int(x) if x.isdigit() else 0 for x in v.replace("-", ".").split(".")])


def _safe(version: str) -> str:
    """A version comes from a bundle's text, and names a directory and a URL."""
    if not version or not all(c.isalnum() or c in ".-+" for c in version) or ".." in version:
        raise SchemaSetError(f"bundle version {version!r} is not a version")
    return version


def schema_source(version: str) -> str:
    """The set's directory when shipped; else the URL."""
    _safe(version)
    for root in _roots("schemas"):
        if (root / version / MANIFEST).is_file():
            return str(root / version)
    return config()["schema_url"].format(version=version)


def column_set_source(version: str) -> str:
    _safe(version)
    for root in _roots("spec"):
        path = root / _COLUMN_SET.format(version=version)
        if path.is_file():
            return str(path)
    return config()["column_set_url"].format(version=version)


def sections_of(version: str) -> tuple[str, ...]:
    """The collections of a default bundle of `version`, in schema order. Raises SchemaSetError if its schema set
    can't load."""
    return root_parts(load_schema_set(schema_source(version)), version_property()).collections


def shape_of(version: str) -> BundleShape:
    """What a default bundle of `version` may hold; a version whose schema set can't load is refused."""
    try:
        return BundleShape(sections_of(version))
    except SchemaSetError as e:
        raise SchemaVersionError(
            f"bundle schema_version {version!r} can't be read: {e}. See the compatibility matrix in the README."
        ) from e


def reader_rules() -> RootRules:
    """How a spec not compiled for a pairing reads a bundle: the default bundle's root, the line layout only."""
    return RootRules(version_property(), shape_of, version_required=True, general_reader=False)
