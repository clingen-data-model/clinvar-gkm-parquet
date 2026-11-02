"""Load a bundle schema_version's schema set: the bundle schema plus the closure of its `$ref`s (ADR 0006).

A set comes from the snapshot a release ships (`schemas/<version>/` in a source tree,
`clinvar_gkm_parquet/_schemas/<version>/` in a wheel), else from a user cache, else from the network, which fills
the cache. Fetched files are JSON data: nothing in them is ever executed.

A set on disk is one file per schema, original bytes, plus MANIFEST.json:

    {"schema_version": "1.1.0", "bundle_id": "<$id>",
     "files": [{"id": "<$id>", "url": "<URL it was fetched from>", "file": "<relative path>", "sha256": "..."}]}

Identity: `SchemaSet.sha256` is over each `$id` and its file bytes, in `$id` order, so it doesn't depend on where
the set was read from.

Known exception: the bundle schema's declared `$id` (.../clinvar-gkm/<version>/json/ClinvarGkmBundle) returned 404
on 2026-11-02, and the file is served at .../clinvar-gkm/<version>/clinvar-gkm-bundle.schema.json (ADR 0006). So
the bundle schema alone may declare an `$id` other than its URL, within its version's folder, and the `$id` it
declares is the set's key for it. Every other file must declare exactly the identifier that reached it.

Only `https://w3id.org/` identifiers are followed, the host the schemas publish under (ADR 0006 §2), so a `$ref` in
a fetched file can't send the tool anywhere else (another host, or a local file).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from . import sources
from .errors import SchemaSetError

BUNDLE_DIR = "https://w3id.org/clingen/schema/clinvar-gkm/{version}/"
BUNDLE_URL = BUNDLE_DIR + "clinvar-gkm-bundle.schema.json"
MANIFEST = "MANIFEST.json"
_VERSION_RE = re.compile(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?")


@dataclass(frozen=True)
class SchemaFile:
    id: str
    url: str
    raw: bytes
    doc: dict

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class SchemaSet:
    version: str
    bundle_id: str
    files: dict[str, SchemaFile]  # by `$id`

    @property
    def bundle(self) -> dict:
        return self.files[self.bundle_id].doc

    @property
    def sha256(self) -> str:
        h = hashlib.sha256()
        for sid in sorted(self.files):
            raw = self.files[sid].raw
            h.update(f"{sid}\n{len(raw)}\n".encode())
            h.update(raw)
        return h.hexdigest()

    def resolve(self, ref: str, base: str) -> tuple[dict, str]:
        """The node `ref` names, read from the schema `base`, and the `$id` of the file that holds it."""
        doc_id, pointer = split_ref(ref, base)
        file = self.files.get(doc_id)
        if file is None:
            raise SchemaSetError(f"schema set {self.version}: {base}: $ref {ref!r}: no schema {doc_id} in the set")
        node = file.doc
        for step in pointer.split("/")[1:] if pointer else ():
            step = urllib.parse.unquote(step).replace("~1", "/").replace("~0", "~")
            if isinstance(node, list) and step.isdigit() and int(step) < len(node):
                node = node[int(step)]
            elif isinstance(node, dict) and step in node:
                node = node[step]
            else:
                raise SchemaSetError(f"schema set {self.version}: {base}: $ref {ref!r}: no {step!r} in {doc_id}")
        if not isinstance(node, dict):
            raise SchemaSetError(f"schema set {self.version}: {base}: $ref {ref!r} is not a schema object")
        return node, doc_id


def split_ref(ref: str, base: str) -> tuple[str, str]:
    """(the absolute `$id` of the file `ref` names, the JSON pointer inside it; '' for the whole file)."""
    absolute, _, fragment = urllib.parse.urljoin(base, ref).partition("#")
    if fragment and not fragment.startswith("/"):
        raise SchemaSetError(f"{base}: $ref {ref!r}: only JSON-pointer fragments (#/...) are understood")
    return absolute, fragment


def refs_of(node: object) -> list[str]:
    """Every `$ref` in a schema document, in document order."""
    out: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                out.append(value)
            elif key not in ("const", "enum", "default", "examples"):  # values, not schema
                out.extend(refs_of(value))
    elif isinstance(node, list):
        for item in node:
            out.extend(refs_of(item))
    return out


def _parse(raw: bytes, where: str) -> dict:
    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise SchemaSetError(f"{where}: not JSON: {e}") from e
    if not isinstance(doc, dict):
        raise SchemaSetError(f"{where}: a schema file must be a JSON object")
    return doc


def _closure(version: str, url: str, fetch: Callable[[str], bytes]) -> SchemaSet:
    """Walk the `$ref`s from the bundle schema at `url`; each file comes from `fetch`, sequentially."""
    bundle_raw = fetch(url)
    bundle = _parse(bundle_raw, url)
    bundle_id = bundle.get("$id")
    if not isinstance(bundle_id, str) or not bundle_id.startswith(BUNDLE_DIR.format(version=version)):
        raise SchemaSetError(f"{url}: the bundle schema declares $id {bundle_id!r}, outside its version's folder")
    files = {bundle_id: SchemaFile(bundle_id, url, bundle_raw, bundle)}
    todo = [(ref, bundle_id) for ref in refs_of(bundle)]
    while todo:
        ref, base = todo.pop(0)
        doc_id, _ = split_ref(ref, base)
        if doc_id in files:
            continue
        parts = urllib.parse.urlsplit(doc_id)
        if parts.scheme != "https" or parts.netloc != "w3id.org" or parts.query:
            raise SchemaSetError(f"schema set {version}: {base}: $ref {ref!r}: not a https://w3id.org/ identifier")
        try:
            raw = fetch(doc_id)
        except SchemaSetError as e:
            raise SchemaSetError(f"schema set {version}: {base}: $ref {ref!r}: {e}") from e
        doc = _parse(raw, doc_id)
        if doc.get("$id") != doc_id:
            raise SchemaSetError(
                f"schema set {version}: {doc_id} (reached from {base}) declares $id {doc.get('$id')!r}"
            )
        files[doc_id] = SchemaFile(doc_id, doc_id, raw, doc)
        todo.extend((r, doc_id) for r in refs_of(doc))
    return SchemaSet(version, bundle_id, files)


def _check_closed(schema_set: SchemaSet, where: str) -> None:
    """Every `$ref` of a set read from disk must name a file and a node of the set."""
    for file in schema_set.files.values():
        for ref in refs_of(file.doc):
            try:
                schema_set.resolve(ref, file.id)
            except SchemaSetError as e:
                raise SchemaSetError(f"{where}: {e}") from e


def read_set(version: str, directory: Path) -> SchemaSet:
    """A set on disk (module docstring). Each file's sha256 and `$id` must equal the manifest's."""
    path = directory / MANIFEST
    try:
        manifest = _parse(path.read_bytes(), str(path))
    except OSError as e:
        raise SchemaSetError(f"{path}: can't read the manifest: {e}") from e
    if manifest.get("schema_version") != version:
        raise SchemaSetError(f"{path}: is for schema_version {manifest.get('schema_version')!r}, not {version}")
    entries = manifest.get("files")
    keys = {"id", "url", "file", "sha256"}
    if not (isinstance(entries, list) and all(isinstance(e, dict) and keys <= set(e) for e in entries)):
        raise SchemaSetError(f"{path}: files must list {sorted(keys)} for each schema")
    files = {}
    for entry in entries:
        file_path = directory / entry["file"]
        try:
            raw = file_path.read_bytes()
        except OSError as e:
            raise SchemaSetError(f"{path}: schema {entry['id']}: can't read {file_path}: {e}") from e
        sha = hashlib.sha256(raw).hexdigest()
        if sha != entry["sha256"]:
            raise SchemaSetError(
                f"{path}: schema {entry['id']}: {file_path} has sha256 {sha}, not {entry['sha256']}"
            )
        doc = _parse(raw, str(file_path))
        if doc.get("$id") != entry["id"]:
            raise SchemaSetError(f"{file_path}: declares $id {doc.get('$id')!r}, not {entry['id']}")
        files[entry["id"]] = SchemaFile(entry["id"], entry["url"], raw, doc)
    schema_set = SchemaSet(version, manifest.get("bundle_id"), files)
    if schema_set.bundle_id not in files:
        raise SchemaSetError(f"{path}: bundle_id {schema_set.bundle_id} is not one of its files")
    _check_closed(schema_set, str(path))
    return schema_set


def write_set(schema_set: SchemaSet, directory: Path) -> None:
    """Write `schema_set` as a set on disk: original bytes, plus MANIFEST.json (written last)."""
    entries = []
    try:
        for sid in sorted(schema_set.files):
            file = schema_set.files[sid]
            rel = _relative_path(file.url)
            target = directory / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(file.raw)
            entries.append({"id": sid, "url": file.url, "file": rel, "sha256": file.sha256})
        manifest = {"schema_version": schema_set.version, "bundle_id": schema_set.bundle_id, "files": entries}
        (directory / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        raise SchemaSetError(f"schema set {schema_set.version}: can't write {directory}: {e}") from e


def _relative_path(url: str) -> str:
    """Where a schema lives inside a set directory: its URL's path."""
    parts = urllib.parse.urlsplit(url)
    rel = urllib.parse.unquote(parts.path).lstrip("/")
    if not rel or ".." in rel.split("/"):
        raise SchemaSetError(f"{url}: can't be a file name in a schema set")
    return rel


def snapshot_dir(version: str) -> Path | None:
    """The shipped snapshot of `version`: in the wheel, or schemas/ in a source tree; None if there is none."""
    for root in (Path(__file__).parent / "_schemas", Path(__file__).resolve().parents[2] / "schemas"):
        if (root / version / MANIFEST).is_file():
            return root / version
    return None


def default_cache_dir() -> Path:
    """$XDG_CACHE_HOME/clinvar-gkm-parquet/schemas, or ~/.cache/... (the fetched sets)."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "clinvar-gkm-parquet" / "schemas"


def _http_fetch(url: str) -> bytes:
    return sources._fetch_bytes(url)


def fetch_set(version: str, *, fetch: Callable[[str], bytes] = _http_fetch) -> SchemaSet:
    """`version`'s schema set from the network: the bundle schema's published URL, then its `$ref` closure."""
    if not _VERSION_RE.fullmatch(version):
        raise SchemaSetError(f"schema_version {version!r} is not a version")

    def checked(url: str) -> bytes:
        try:
            return fetch(url)
        except SchemaSetError:
            raise
        except Exception as e:
            raise SchemaSetError(f"can't fetch {url}: {e}") from e

    return _closure(version, BUNDLE_URL.format(version=version), checked)


def load_schema_set(
    version: str, *, cache_dir: Path | None = None, fetch: Callable[[str], bytes] = _http_fetch
) -> SchemaSet:
    """`version`'s schema set: the shipped snapshot, else the cache, else fetched and cached."""
    snapshot = snapshot_dir(version)
    if snapshot is not None:
        return read_set(version, snapshot)
    cached = (cache_dir or default_cache_dir()) / version
    if (cached / MANIFEST).is_file():
        return read_set(version, cached)
    schema_set = fetch_set(version, fetch=fetch)
    write_set(schema_set, cached)
    return schema_set
