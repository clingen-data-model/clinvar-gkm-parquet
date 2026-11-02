"""Load a bundle schema set: a root schema plus the closure of its `$ref`s, for any producer (ADR 0006).

The root is a local file, an https:// URL, or a directory holding a set on disk (`read_set`). A set is fetched once
and cached in a user cache, keyed by the root's URL or, for a local file, its bytes. Fetched files are JSON data:
nothing in them is ever executed.

Only `https://w3id.org/` identifiers are followed from a `$ref`, the host GA4GH schemas publish under, so a `$ref`
in a fetched file can't send the tool anywhere else (another host, or a local file). The root itself can be
anywhere, and its `$id` can be of any form (a URN, or a URL other than the one it was read from): the `$id` it
declares is the set's key for it, and what a column set pairs with. Every other file must declare exactly the
identifier that reached it.

A set on disk is one file per schema, original bytes, plus MANIFEST.json:

    {"bundle_id": "<the root's $id>",
     "files": [{"id": "<$id>", "url": "<where it was read from>", "file": "<relative path>", "sha256": "..."}]}

Identity: `SchemaSet.sha256` is over each `$id` and its file bytes, in `$id` order, so it doesn't depend on where
the set was read from.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from .errors import SchemaSetError
from .transport import TOOL_NAME, fetch_bytes, is_url

MANIFEST = "MANIFEST.json"
FOLLOWED_HOST = "w3id.org"


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
    bundle_id: str  # the root schema's $id
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
            raise SchemaSetError(
                f"schema set {self.bundle_id}: {base}: $ref {ref!r}: no schema {doc_id} in the set"
            )
        node = file.doc
        for step in pointer.split("/")[1:] if pointer else ():
            step = urllib.parse.unquote(step).replace("~1", "/").replace("~0", "~")
            if isinstance(node, list) and step.isdigit() and int(step) < len(node):
                node = node[int(step)]
            elif isinstance(node, dict) and step in node:
                node = node[step]
            else:
                raise SchemaSetError(f"schema set {self.bundle_id}: {base}: $ref {ref!r}: no {step!r} in {doc_id}")
        if not isinstance(node, dict):
            raise SchemaSetError(f"schema set {self.bundle_id}: {base}: $ref {ref!r} is not a schema object")
        return node, doc_id


def split_ref(ref: str, base: str) -> tuple[str, str]:
    """(the `$id` of the file `ref` names, the JSON pointer inside it; '' for the whole file)."""
    if ref.startswith("#"):  # within the file, whatever form its $id has (urljoin can't join a URN)
        absolute, fragment = base, ref[1:]
    else:
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


def _closure(root_url: str, root_raw: bytes, fetch: Callable[[str], bytes]) -> SchemaSet:
    """Walk the `$ref`s from the root schema; each file comes from `fetch`, sequentially."""
    root = _parse(root_raw, root_url)
    root_id = root.get("$id")
    if not isinstance(root_id, str) or not root_id:
        raise SchemaSetError(f"{root_url}: the root schema declares no $id")
    files = {root_id: SchemaFile(root_id, root_url, root_raw, root)}
    todo = [(ref, root_id) for ref in refs_of(root)]
    while todo:
        ref, base = todo.pop(0)
        doc_id, _ = split_ref(ref, base)
        if doc_id in files:
            continue
        parts = urllib.parse.urlsplit(doc_id)
        if parts.scheme != "https" or parts.netloc != FOLLOWED_HOST or parts.query:
            raise SchemaSetError(
                f"schema set {root_id}: {base}: $ref {ref!r}: not a https://{FOLLOWED_HOST}/ identifier"
            )
        try:
            raw = fetch(doc_id)
        except SchemaSetError as e:
            raise SchemaSetError(f"schema set {root_id}: {base}: $ref {ref!r}: {e}") from e
        doc = _parse(raw, doc_id)
        if doc.get("$id") != doc_id:
            raise SchemaSetError(
                f"schema set {root_id}: {doc_id} (reached from {base}) declares $id {doc.get('$id')!r}"
            )
        files[doc_id] = SchemaFile(doc_id, doc_id, raw, doc)
        todo.extend((r, doc_id) for r in refs_of(doc))
    return SchemaSet(root_id, files)


def _check_closed(schema_set: SchemaSet, where: str) -> None:
    """Every `$ref` of a set read from disk must name a file and a node of the set."""
    for file in schema_set.files.values():
        for ref in refs_of(file.doc):
            try:
                schema_set.resolve(ref, file.id)
            except SchemaSetError as e:
                raise SchemaSetError(f"{where}: {e}") from e


def read_set(directory: Path) -> SchemaSet:
    """A set on disk (module docstring). Each file's sha256 and `$id` must equal the manifest's."""
    directory = Path(directory)
    path = directory / MANIFEST
    try:
        manifest = _parse(path.read_bytes(), str(path))
    except OSError as e:
        raise SchemaSetError(f"{path}: can't read the manifest: {e}") from e
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
    schema_set = SchemaSet(manifest.get("bundle_id"), files)
    if schema_set.bundle_id not in files:
        raise SchemaSetError(f"{path}: bundle_id {schema_set.bundle_id} is not one of its files")
    _check_closed(schema_set, str(path))
    return schema_set


def write_set(schema_set: SchemaSet, directory: Path) -> None:
    """Write `schema_set` as a set on disk: original bytes, plus MANIFEST.json (written last)."""
    directory = Path(directory)
    entries = []
    try:
        for sid in sorted(schema_set.files):
            file = schema_set.files[sid]
            rel = _relative_path(file.url)
            target = directory / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(file.raw)
            entries.append({"id": sid, "url": file.url, "file": rel, "sha256": file.sha256})
        manifest = {"bundle_id": schema_set.bundle_id, "files": entries}
        (directory / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        raise SchemaSetError(f"schema set {schema_set.bundle_id}: can't write {directory}: {e}") from e


def _relative_path(url: str) -> str:
    """Where a schema lives inside a set directory: its URL's path, or a local root's file name."""
    parts = urllib.parse.urlsplit(url)
    rel = urllib.parse.unquote(parts.path).lstrip("/") if parts.scheme else url
    if not rel or ".." in rel.split("/"):
        raise SchemaSetError(f"{url}: can't be a file name in a schema set")
    return rel


def default_cache_dir() -> Path:
    """$XDG_CACHE_HOME/<tool>, or ~/.cache/<tool>: fetched schema sets and column sets."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / TOOL_NAME


def _read_root(source: str, fetch: Callable[[str], bytes]) -> tuple[str, bytes]:
    """(what a set records as the root's location, its bytes)."""
    try:
        if is_url(source):
            return source, fetch(source)
        return Path(source).name, Path(source).read_bytes()
    except SchemaSetError:
        raise
    except Exception as e:
        raise SchemaSetError(f"can't read the root schema {source}: {e}") from e


def _fetch(fetch: Callable[[str], bytes]) -> Callable[[str], bytes]:
    def checked(url: str) -> bytes:
        try:
            return fetch(url)
        except SchemaSetError:
            raise
        except Exception as e:
            raise SchemaSetError(f"can't fetch {url}: {e}") from e

    return checked


def fetch_set(source: str, *, fetch: Callable[[str], bytes] = fetch_bytes) -> SchemaSet:
    """The schema set whose root is `source`, a file or URL, with its `$ref` closure read from the network."""
    checked = _fetch(fetch)
    root_url, root_raw = _read_root(str(source), checked)
    return _closure(root_url, root_raw, checked)


def load_schema_set(
    source: str | Path, *, cache_dir: Path | None = None, fetch: Callable[[str], bytes] = fetch_bytes
) -> SchemaSet:
    """The schema set whose root is `source`: a directory holding a set on disk; else a file or URL, read from the
    cache when it is there, else fetched and cached. A directory is read once per process."""
    source = str(source)
    if not is_url(source) and Path(source).is_dir():
        return _read_once(Path(source))
    checked = _fetch(fetch)
    if is_url(source):
        key = hashlib.sha256(source.encode()).hexdigest()
        directory = (cache_dir or default_cache_dir()) / "schemas" / key
        if (directory / MANIFEST).is_file():
            return _read_once(directory)
        root_url, root_raw = _read_root(source, checked)
    else:
        root_url, root_raw = _read_root(source, checked)
        key = hashlib.sha256(root_raw).hexdigest()
        directory = (cache_dir or default_cache_dir()) / "schemas" / key
        if (directory / MANIFEST).is_file():
            return _read_once(directory)
    write_set(_closure(root_url, root_raw, checked), directory)
    return _read_once(directory)


@cache
def _read_once(directory: Path) -> SchemaSet:
    return read_set(directory)
