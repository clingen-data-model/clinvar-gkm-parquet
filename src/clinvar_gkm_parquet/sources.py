"""Resolve inputs: local paths and URLs, checksums, the line index, delta manifests (design plan §4.1).

Owned by the input-side work package. This is the ClinVar-GKM release layer; reading bytes is transport.py. Network
rules: CLAUDE.md, .claude/rules/python.md. Unit tests never touch the network.

The index and manifest shapes are the ones ClinVar-GKM writes (`src/scripts/r2_layout.py` build_index,
checkpoint_for, plan_move_10; `src/scripts/build-delta-manifest.py`; @ origin/1.0 25344ba):

- line index: `datasets.monthly[]` {name, path, size, sha256, release (YYYY-MM), source_release, modified,
  latest}, `datasets.weekly[]` {name, path, size, sha256, release (YYYY-MM-DD), modified, latest},
  `deltas[]` {release, month, path (folder), manifest (path or null), bundle {path, sha256, size}, modified},
  `archives` {"YYYY": {monthly: [...], deltas: [...]}}, `latest_release`, `base_url`. Paths are relative to
  base_url and start with the line (`v1/1.1/...`). The `latest: true` entries are the 00-latest aliases.
- delta manifest: {release, baseline_release, compare_release, pipeline_version, checkpoint_full {path,
  release (the monthly's source_release), month, sha256} | null, sections {<section>: {added, updated,
  deleted: [key, ...]}}, counts, schema_version, bundle {name, path, size, sha256}, sha256}.
- frozen line MANIFEST.json: {line, files: [{key, size, sha256, ...}], ...}; `key` is the bucket key.
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.parse
from pathlib import Path

from .errors import InputError
from .model import BuildInputs, DeltaManifest, InputFile
from .transport import fetch_bytes, open_stream, read_all
from .transport import is_url as _is_url

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://data.clinvar-gkm.org"
DEFAULT_INDEX_URL = f"{DEFAULT_BASE_URL}/v1/1.1/index.json"  # provisional path (design plan §2.1)

ALIASES = {
    "clinvar-gkm_00-latest.json.gz": "monthly",
    "clinvar-gkm_00-latest_weekly.json.gz": "weekly",
}

_SHA = re.compile(r"[0-9a-fA-F]{64}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DATED_NAME = re.compile(
    r"clinvar-gkm(?:-delta)?_(\d{4})-(\d{2})(\d{2})(?:\.json\.gz|\.manifest\.json)",
)


def _load_json(source: str, what: str):
    data = read_all(source)
    try:
        return json.loads(data)
    except ValueError as e:
        raise InputError(f"{source}: not a valid {what} (JSON: {e})") from e


# ---------------------------------------------------------------------------------------------------------------
# Index


def load_index(source: str) -> dict:
    """The line index (v1/1.1/index.json), parsed."""
    index = _load_json(source, "line index")
    ds = index.get("datasets") if isinstance(index, dict) else None
    if not (
        isinstance(ds, dict)
        and isinstance(ds.get("monthly"), list)
        and isinstance(ds.get("weekly"), list)
        and isinstance(index.get("deltas"), list)
        and isinstance(index.get("archives", {}), dict)
    ):
        raise InputError(f"{source}: not a line index (expected datasets.monthly, datasets.weekly, deltas)")
    return index


def load_frozen_manifest(source: str) -> dict:
    """A frozen line's MANIFEST.json (size + sha256 per file), parsed."""
    man = _load_json(source, "frozen-line MANIFEST.json")
    if not (isinstance(man, dict) and isinstance(man.get("files"), list)):
        raise InputError(f"{source}: not a frozen-line MANIFEST.json (expected files[])")
    return man


def _monthly_pool(index: dict) -> list[dict]:
    """Every dated official monthly (top level and archives), excluding the 00-latest alias."""
    pool = [m for m in index["datasets"]["monthly"] if not m.get("latest")]
    pool += [m for y in (index.get("archives") or {}).values() for m in y.get("monthly", [])]
    return pool


def _delta_pool(index: dict) -> list[dict]:
    pool = list(index["deltas"])
    pool += [d for y in (index.get("archives") or {}).values() for d in y.get("deltas", [])]
    return pool


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def _find_entry(index: dict, name: str) -> dict | None:
    """{name, path, size, sha256, release} for a dated file named `name` in the index; `release` is a monthly's
    source_release, a weekly's or delta's release."""
    for m in _monthly_pool(index):
        if m.get("name") == name:
            return {**m, "release": m.get("source_release")}
    for w in index["datasets"]["weekly"]:
        if not w.get("latest") and w.get("name") == name:
            return dict(w)
    for d in _delta_pool(index):
        b = d.get("bundle") or {}
        if b.get("path") and _basename(b["path"]) == name:
            return {
                "name": name,
                "path": b["path"],
                "size": b.get("size"),
                "sha256": b.get("sha256"),
                "release": d.get("release"),
            }
        if d.get("manifest") and _basename(d["manifest"]) == name:
            return {"name": name, "path": d["manifest"], "size": None, "sha256": None, "release": d.get("release")}
    return None


def _resolve_alias(index: dict, kind: str, name: str, sha: str | None, size: int | None) -> dict:
    """The dated entry an alias (00-latest / 00-latest_weekly) has the same bytes as."""
    if kind == "monthly":
        pool = [{**m, "release": m.get("source_release")} for m in _monthly_pool(index)]
        latest = [m for m in index["datasets"]["monthly"] if m.get("latest")]
    else:
        pool = [dict(w) for w in index["datasets"]["weekly"] if not w.get("latest")]
        latest = [w for w in index["datasets"]["weekly"] if w.get("latest")]
    if sha is None and latest:
        sha = latest[0].get("sha256")
        if sha is None and latest[0].get("release"):
            # build_index sets the alias entry's release to that of the dated file it equals.
            want = latest[0]["release"]
            hit = [e for e in pool if (kind == "monthly" and _label(e) == want) or e.get("release") == want]
            if len(hit) == 1:
                return _check_alias_size(hit[0], name, size)
    hits = [e for e in pool if sha and (e.get("sha256") or "").lower() == sha.lower()]
    if len(hits) != 1:
        raise InputError(
            f"{name}: is an alias, and the line index names no single dated {kind} file with sha256 {sha}; "
            "pass the dated file instead"
        )
    return _check_alias_size(hits[0], name, size)


def _label(monthly: dict) -> str | None:
    m = re.search(r"_(\d{4}-\d{2})\.json\.gz$", monthly.get("name", ""))
    return m.group(1) if m else None


def _check_alias_size(entry: dict, name: str, size: int | None) -> dict:
    if size is not None and entry.get("size") is not None and entry["size"] != size:
        raise InputError(
            f"{name}: {size} bytes, but the dated file it should equal ({entry['name']}) is {entry['size']} bytes "
            "in the line index; this copy is not the current alias"
        )
    return entry


def _release_from_name(name: str) -> str | None:
    """YYYY-MM-DD from a weekly full, delta or manifest name. A monthly's name has only its label (YYYY-MM),
    not the release it was built from, so it gives None."""
    m = _DATED_NAME.fullmatch(name)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


# ---------------------------------------------------------------------------------------------------------------
# Checksums


def parse_sidecar(text: str, name: str, *, where: str) -> str:
    """The sha256 from a `<file>.sha256` sidecar in `sha256sum` format (`<hex>  <name>` or `<hex> *<name>`;
    a bare `<hex>` is accepted too). A file name, when present, must be `name`."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    m = (
        re.fullmatch(r"([0-9a-fA-F]{64})(?:[ \t]+\*?(\S(?:.*\S)?))?", lines[0].strip())
        if len(lines) == 1
        else None
    )
    if not m:
        raise InputError(f"{where}: not a sha256sum line for one file")
    if m.group(2) and m.group(2) != name and name not in ALIASES:
        raise InputError(f"{where}: names {m.group(2)!r}, not {name!r}")
    return m.group(1).lower()


def _sidecar_sha(source: str, name: str, is_url: bool) -> str | None:
    side = source + ".sha256"
    if is_url:
        data = fetch_bytes(side, allow_404=True)
        if data is None:
            return None
    else:
        if not os.path.isfile(side):
            return None
        data = Path(side).read_bytes()
    return parse_sidecar(data.decode("utf-8", "replace"), name, where=side)


def _frozen_sha(frozen_manifest: dict, name: str, path: str | None) -> str | None:
    for f in frozen_manifest.get("files", []):
        key = f.get("key") or ""
        if (path and key == path) or _basename(key) == name:
            sha = f.get("sha256")
            return sha.lower() if isinstance(sha, str) and _SHA.fullmatch(sha) else None
    return None


# ---------------------------------------------------------------------------------------------------------------
# Resolution


def resolve_input(
    source: str,
    *,
    sha256: str | None = None,
    index: dict | None = None,
    frozen_manifest: dict | None = None,
) -> InputFile:
    """A path or URL -> InputFile, with the expected sha256 from the first source that has one: `sha256`
    (explicit), the `<file>.sha256` sidecar, the line index, a frozen line's MANIFEST.json.

    An alias (clinvar-gkm_00-latest / 00-latest_weekly) needs `index`: it resolves to the dated file with the
    same sha256 and size, whose name and release are recorded (and, for a URL, whose dated URL is read).
    """
    is_url = _is_url(source)
    url_path = urllib.parse.urlsplit(source).path if is_url else source
    name = _basename(urllib.parse.unquote(url_path) if is_url else url_path)
    size: int | None = None
    if not is_url:
        try:
            size = os.stat(source).st_size
        except OSError as e:
            raise InputError(f"{source}: {e.strerror or e}") from e

    expected, sha_src = None, None
    if sha256 is not None:
        if not _SHA.fullmatch(sha256):
            raise InputError(f"--sha256 {sha256!r} is not a 64-digit hex sha256")
        expected, sha_src = sha256.lower(), "explicit"
    else:
        side = _sidecar_sha(source, name, is_url)
        if side:
            expected, sha_src = side, "sidecar"

    entry = None
    if name in ALIASES:
        if index is None:
            raise InputError(
                f"{name} is an alias that moves every release; pass the line index so it can be resolved to the "
                "dated file (or pass the dated file)"
            )
        entry = _resolve_alias(index, ALIASES[name], name, expected, size)
        name = entry["name"]
        if is_url:
            parts = urllib.parse.urlsplit(source)
            source = f"{parts.scheme}://{parts.netloc}/{entry['path'].lstrip('/')}"
    elif index is not None:
        entry = _find_entry(index, name)

    release = entry.get("release") if entry else _release_from_name(name)
    if expected is None and entry and entry.get("sha256"):
        expected, sha_src = entry["sha256"].lower(), "index"
    if expected is None and frozen_manifest is not None:
        frozen = _frozen_sha(frozen_manifest, name, entry.get("path") if entry else None)
        if frozen:
            expected, sha_src = frozen, "frozen-manifest"
    if size is None and entry:
        size = entry.get("size")
    return InputFile(
        source=source, name=name, release=release, expected_sha256=expected, sha256_source=sha_src, size=size
    )


def load_manifest(inp: InputFile) -> DeltaManifest:
    """A delta manifest, validated: release and baseline_release are dates, every `deleted` entry is a key string.
    Its sections are checked against the bundle's schema_version when the delta is read (replay.read_delta): the
    manifest doesn't state one."""
    with open_stream(inp) as fh:
        data = fh.read()
    if inp.expected_sha256:
        import hashlib

        got = hashlib.sha256(data).hexdigest()
        if got != inp.expected_sha256.lower():
            raise InputError(f"{inp.name}: sha256 mismatch: computed {got}, expected {inp.expected_sha256}")
    try:
        raw = json.loads(data)
    except ValueError as e:
        raise InputError(f"{inp.name}: not a valid delta manifest (JSON: {e})") from e

    def bad(msg: str) -> InputError:
        return InputError(f"{inp.name}: delta manifest: {msg}")

    if not isinstance(raw, dict):
        raise bad("not a JSON object")
    release, baseline = raw.get("release"), raw.get("baseline_release")
    if not (isinstance(release, str) and _DATE.fullmatch(release)):
        raise bad(f"release {release!r} is not YYYY-MM-DD")
    if baseline is not None and not (isinstance(baseline, str) and _DATE.fullmatch(baseline)):
        raise bad(f"baseline_release {baseline!r} is not YYYY-MM-DD or null")
    if inp.release and inp.release != release:
        raise bad(f"release {release} does not match the file's release {inp.release}")
    sections = raw.get("sections")
    if not isinstance(sections, dict):
        raise bad("sections is missing or not an object")
    deleted: dict[str, tuple[str, ...]] = {}
    for sec, info in sections.items():
        keys = info.get("deleted") if isinstance(info, dict) else None
        if not (isinstance(keys, list) and all(isinstance(k, str) for k in keys)):
            raise bad(f"sections.{sec}.deleted is missing or not a list of key strings")
        if keys:
            deleted[sec] = tuple(keys)
    cp = raw.get("checkpoint_full")
    if cp is not None and not isinstance(cp, dict):
        raise bad("checkpoint_full is not an object or null")
    return DeltaManifest(release=release, baseline_release=baseline, deleted=deleted, checkpoint_full=cp, raw=raw)


def resolve_release(index: dict, release: str, *, base_url: str) -> BuildInputs:
    """`--release latest|YYYY-MM-DD`: the weekly full if it is still published, else the replay rule (§4.3):
    the newest official monthly whose source_release <= W, then every delta with source_release < release <= W,
    oldest first. Checksums come from the index (its sha256 fields are read from the published sidecars).
    The delta chain itself is checked later, from the manifests (replay.check_chain)."""
    base = base_url.rstrip("/")
    latest = index.get("latest_release")
    target = latest if release == "latest" else release
    if not (isinstance(target, str) and _DATE.fullmatch(target)):
        if release == "latest":
            raise InputError("the line index has no latest_release")
        raise InputError(f"--release {release!r}: expected 'latest' or YYYY-MM-DD")
    if latest and target > latest:
        raise InputError(f"--release {target}: the newest release in the line index is {latest}")

    def file_for(path: str, rel: str | None, sha: str | None, size: int | None) -> InputFile:
        return InputFile(
            source=f"{base}/{path.lstrip('/')}",
            name=_basename(path),
            release=rel,
            expected_sha256=sha.lower() if sha else None,
            sha256_source="index" if sha else None,
            size=size,
        )

    for w in index["datasets"]["weekly"]:
        if not w.get("latest") and w.get("release") == target:
            return BuildInputs(
                full=file_for(w["path"], target, w.get("sha256"), w.get("size")), full_source_release=target
            )

    monthlies = _monthly_pool(index)
    unknown = [m.get("name") for m in monthlies if not m.get("source_release")]
    if unknown:
        log.warning("line index: source_release unknown for %s; they can't start a replay", ", ".join(unknown))
    known = [m for m in monthlies if m.get("source_release")]
    deltas = _delta_pool(index)
    releases = {d.get("release") for d in deltas} | {m["source_release"] for m in known}
    releases |= {w.get("release") for w in index["datasets"]["weekly"] if not w.get("latest")}
    if target not in releases:
        earlier = sorted(r for r in releases if r and r < target)
        hint = f"; the nearest earlier release is {earlier[-1]}" if earlier else ""
        raise InputError(f"--release {target}: no such release in the line index{hint}")
    cands = [m for m in known if m["source_release"] <= target]
    if not cands:
        raise InputError(
            f"--release {target}: no official monthly in the line index was built from a release on or before it"
        )
    monthly = max(cands, key=lambda m: (m["source_release"], m.get("release") or ""))
    start = monthly["source_release"]
    pairs = []
    for d in sorted((d for d in deltas if start < d.get("release", "") <= target), key=lambda d: d["release"]):
        b = d.get("bundle") or {}
        if not b.get("path") or not d.get("manifest"):
            raise InputError(f"line index: the {d['release']} delta has no bundle or no manifest listed")
        pairs.append(
            (
                file_for(b["path"], d["release"], b.get("sha256"), b.get("size")),
                file_for(d["manifest"], d["release"], None, None),
            )
        )
    full = file_for(monthly["path"], start, monthly.get("sha256"), monthly.get("size"))
    return BuildInputs(full=full, full_source_release=start, deltas=tuple(pairs))
