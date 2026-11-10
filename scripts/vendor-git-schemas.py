#!/usr/bin/env python3
"""Write a schema set on disk from a commit of a local git clone, without touching its checkout.

For a producer's schemas that are not published yet (a pull request's branch). The candidate files are found the
way ClinVar-GKM's `load_registry` finds them: every file under <tree> (following symlinks, into submodules) that
sits in a `json/` folder, parses as a JSON object and declares `$id`. The set is the `$ref` closure of <root> over
those files, read through the tool's own loader (schemas.py), so it holds exactly what a build or `validate` would
see.

Everything is read from git objects at the given commit (`git ls-tree`, `git cat-file`): a symlink is followed
inside the commit, and a submodule is read at the commit its gitlink records, from the submodule's local clone.
Nothing is fetched. Each file's `url` in MANIFEST.json is where it can be fetched at that commit
(raw.githubusercontent.com), and `file` is its `$id`'s path. Two candidates with one `$id` and different bytes stop
the script.

Usage:
    python scripts/vendor-git-schemas.py <repo> <commit> <root path in repo> <directory> [--tree schema]
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import shutil
import subprocess
import sys
import urllib.parse
from pathlib import Path

from clinvar_gkm_parquet.errors import ToolError
from clinvar_gkm_parquet.schemas import MANIFEST, _closure, read_set


def _git(repo: Path, *args: str) -> bytes:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True).stdout


def _raw_base(repo: Path, commit: str) -> str:
    """https://raw.githubusercontent.com/<owner>/<name>/<commit>/ for the clone's origin."""
    origin = _git(repo, "config", "--get", "remote.origin.url").decode().strip()
    m = re.fullmatch(r"(?:https://github\.com/|git@github\.com:)([^/]+)/(.+?)(?:\.git)?/?", origin)
    if not m:
        raise SystemExit(f"{repo}: origin {origin!r} is not a GitHub repository")
    return f"https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}/{commit}/"


class Tree:
    """One repository at one commit: its entries by path, read once."""

    def __init__(self, repo: Path, commit: str):
        self.repo, self.commit = repo, _git(repo, "rev-parse", f"{commit}^{{commit}}").decode().strip()
        self.base = _raw_base(repo, self.commit)
        self.entries: dict[str, tuple[str, str]] = {}  # path -> (mode, object)
        for line in _git(repo, "ls-tree", "-r", "-z", self.commit).split(b"\0"):
            if line:
                meta, path = line.decode().split("\t", 1)
                mode, _, obj = meta.split()
                self.entries[path] = (mode, obj)

    def blob(self, obj: str) -> bytes:
        return _git(self.repo, "cat-file", "blob", obj)


def walk(tree: Tree, prefix: str, out: dict[str, tuple[bytes, str]], seen: set[tuple[str, str]]) -> None:
    """Every file at or under `prefix` -> out[logical path] = (bytes, url), following symlinks and submodules."""
    if (tree.commit, prefix) in seen:
        return
    seen.add((tree.commit, prefix))
    for path, (mode, obj) in tree.entries.items():
        if mode == "160000" and (prefix + "/").startswith(path + "/"):  # the prefix is inside a submodule
            sub = Tree(tree.repo / path, obj)
            walk(sub, prefix[len(path) :].lstrip("/"), out, seen)
            return
        if mode == "120000" and (prefix + "/").startswith(path + "/"):  # the prefix runs through a symlink
            target = posixpath.normpath(posixpath.join(posixpath.dirname(path), tree.blob(obj).decode()))
            walk(tree, target + prefix[len(path) :], out, seen)
            return
    under = prefix.rstrip("/") + "/" if prefix else ""
    for path, (mode, obj) in sorted(tree.entries.items()):
        if prefix and path != prefix and not path.startswith(under):
            continue
        if mode == "120000":
            target = posixpath.normpath(posixpath.join(posixpath.dirname(path), tree.blob(obj).decode()))
            walk(tree, target, out, seen)
        elif mode == "160000":
            walk(Tree(tree.repo / path, obj), "", out, seen)
        elif posixpath.basename(posixpath.dirname(path)) == "json" and not path.endswith(".md"):
            out.setdefault(f"{tree.base}{path}", (tree.blob(obj), f"{tree.base}{urllib.parse.quote(path)}"))


def candidates(files: dict[str, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]]:
    """`$id` -> (bytes, url), for each file that parses as a JSON object declaring `$id`."""
    by_id: dict[str, tuple[bytes, str]] = {}
    for raw, url in files.values():
        try:
            doc = json.loads(raw)
        except ValueError:
            continue
        sid = doc.get("$id") if isinstance(doc, dict) else None
        if not isinstance(sid, str) or not sid:
            continue
        if sid in by_id and by_id[sid][0] != raw:
            raise SystemExit(f"$id {sid} has two different files: {by_id[sid][1]} and {url}")
        by_id.setdefault(sid, (raw, url))
    return by_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("repo", type=Path, help="a local clone (its checkout is not read or changed)")
    parser.add_argument("commit")
    parser.add_argument("root", help="the bundle schema's path in the repository")
    parser.add_argument("directory", type=Path, help="where to write the set (replaced if it exists)")
    parser.add_argument("--tree", default="schema", help="the folder whose json/ files are candidates")
    args = parser.parse_args()

    top = Tree(args.repo, args.commit)
    files: dict[str, tuple[bytes, str]] = {}
    walk(top, args.tree, files, set())
    pool = candidates(files)
    mode, obj = top.entries[args.root]
    root_raw, root_url = top.blob(obj), top.base + urllib.parse.quote(args.root)

    def fetch(sid: str) -> bytes:
        if sid not in pool:
            raise ToolError(f"no file under {args.tree}/ declares $id {sid}")
        return pool[sid][0]

    try:
        schema_set = _closure(root_url, root_raw, fetch)
    except ToolError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if args.directory.exists():
        shutil.rmtree(args.directory)
    entries = []
    for sid in sorted(schema_set.files):
        f = schema_set.files[sid]
        rel = urllib.parse.urlsplit(sid).path.lstrip("/")
        target = args.directory / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(f.raw)
        url = root_url if sid == schema_set.bundle_id else pool[sid][1]
        entries.append({"id": sid, "url": url, "file": rel, "sha256": f.sha256})
    manifest = {"bundle_id": schema_set.bundle_id, "files": entries}
    (args.directory / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    check = read_set(args.directory)
    print(
        f"{args.directory}: {len(check.files)} files from {len(pool)} candidates, commit {top.commit}, "
        f"schema set sha256 {check.sha256}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
