"""The schema set loader (ADR 0006): the shipped snapshot, the cache, the fetch of a `$ref` closure, its errors.

The fetch tests run on small schemas in a dict: they test the walk, not the schemas. One opt-in test reads the real
1.1.0 set by URL.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from clinvar_gkm_parquet.errors import SchemaSetError
from clinvar_gkm_parquet.schemas import (
    BUNDLE_URL,
    MANIFEST,
    fetch_set,
    load_schema_set,
    read_set,
    snapshot_dir,
    split_ref,
    write_set,
)

VERSION = "8.8.0"
BUNDLE = BUNDLE_URL.format(version=VERSION)
W3 = "https://w3id.org/x"


def _site(**overrides) -> dict[str, bytes]:
    """A small set: the bundle refers to A by absolute URL, A to B by a root-relative ref and into itself."""
    docs = {
        BUNDLE: {"$id": BUNDLE, "properties": {"a": {"$ref": f"{W3}/A"}, "b": {"$ref": f"{W3}/B#/$defs/C"}}},
        f"{W3}/A": {
            "$id": f"{W3}/A",
            "$defs": {"D": {"type": "string"}},
            "properties": {
                "b": {"$ref": "/x/B"},
                "d": {"$ref": "#/$defs/D"},
                "e": {"const": {"$ref": "not a ref"}},
            },
        },
        f"{W3}/B": {"$id": f"{W3}/B", "type": "object", "$defs": {"C": {"type": "integer"}}},
        **overrides,
    }
    return {url: json.dumps(doc).encode() for url, doc in docs.items() if doc is not None}


def _fetcher(site: dict[str, bytes], calls: list[str] | None = None):
    def fetch(url: str) -> bytes:
        if calls is not None:
            calls.append(url)
        if url not in site:
            raise FileNotFoundError(f"{url}: HTTP 404")
        return site[url]

    return fetch


def test_fetch_walks_the_closure_once_per_file():
    calls: list[str] = []
    s = fetch_set(VERSION, fetch=_fetcher(_site(), calls))
    assert set(s.files) == {BUNDLE, f"{W3}/A", f"{W3}/B"}
    assert calls == [BUNDLE, f"{W3}/A", f"{W3}/B"]
    assert s.bundle["$id"] == BUNDLE


def test_resolve_follows_absolute_relative_and_pointer_refs():
    s = fetch_set(VERSION, fetch=_fetcher(_site()))
    assert s.resolve("/x/B", f"{W3}/A") == (s.files[f"{W3}/B"].doc, f"{W3}/B")
    assert s.resolve("#/$defs/D", f"{W3}/A") == ({"type": "string"}, f"{W3}/A")
    assert s.resolve(f"{W3}/B#/$defs/C", BUNDLE) == ({"type": "integer"}, f"{W3}/B")


@pytest.mark.parametrize(
    "ref, message",
    [("/x/Other", "no schema https://w3id.org/x/Other in the set"), ("#/$defs/Nope", "no 'Nope' in")],
)
def test_resolve_names_what_is_missing(ref, message):
    s = fetch_set(VERSION, fetch=_fetcher(_site()))
    with pytest.raises(SchemaSetError, match=message):
        s.resolve(ref, f"{W3}/A")


def test_split_ref_refuses_a_plain_name_fragment():
    assert split_ref("/x/B#/$defs/C", f"{W3}/A") == (f"{W3}/B", "/$defs/C")
    with pytest.raises(SchemaSetError, match="only JSON-pointer fragments"):
        split_ref("#anchor", f"{W3}/A")


def test_an_unfetchable_file_is_an_error_naming_both_ends():
    with pytest.raises(
        SchemaSetError, match=r"bundle.schema.json: \$ref '.*x/B#/\$defs/C': can't fetch .*x/B.*HTTP 404"
    ):
        fetch_set(VERSION, fetch=_fetcher(_site(**{f"{W3}/B": None})))


def test_an_unfetchable_bundle_schema_is_an_error_naming_its_url():
    with pytest.raises(SchemaSetError, match="can't fetch .*clinvar-gkm/8.8.0/clinvar-gkm-bundle.schema.json"):
        fetch_set(VERSION, fetch=_fetcher({}))


def test_a_file_whose_id_differs_from_the_ref_is_an_error():
    site = _site(**{f"{W3}/B": {"$id": f"{W3}/NotB"}})
    with pytest.raises(SchemaSetError, match=r"x/B \(reached from .*\) declares \$id .*NotB"):
        fetch_set(VERSION, fetch=_fetcher(site))


def test_only_the_bundle_schema_may_declare_an_id_other_than_its_url():
    declared = BUNDLE.replace("clinvar-gkm-bundle.schema.json", "json/ClinvarGkmBundle")
    site = _site(**{BUNDLE: {"$id": declared, "properties": {"a": {"$ref": f"{W3}/A"}}}})
    s = fetch_set(VERSION, fetch=_fetcher(site))
    assert s.bundle_id == declared and s.files[declared].url == BUNDLE


def test_a_bundle_schema_declaring_another_versions_id_is_an_error():
    site = _site(**{BUNDLE: {"$id": BUNDLE.replace(VERSION, "1.1.0")}})
    with pytest.raises(SchemaSetError, match=r"declares \$id .*1\.1\.0.*outside its version's folder"):
        fetch_set(VERSION, fetch=_fetcher(site))


@pytest.mark.parametrize(
    "ref",
    ["file:///etc/passwd", "http://w3id.org/x/A", "https://example.org/x/A", "https://w3id.org/x/A?y=1"],
)
def test_a_ref_outside_w3id_org_is_never_fetched(ref):
    calls: list[str] = []
    site = _site(**{BUNDLE: {"$id": BUNDLE, "properties": {"a": {"$ref": ref}}}})
    with pytest.raises(SchemaSetError, match="not a https://w3id.org/ identifier"):
        fetch_set(VERSION, fetch=_fetcher(site, calls))
    assert calls == [BUNDLE]


def test_not_json_is_an_error_naming_the_file():
    site = _site()
    site[f"{W3}/B"] = b"<html>"
    with pytest.raises(SchemaSetError, match=r"x/B: not JSON"):
        fetch_set(VERSION, fetch=_fetcher(site))


def test_a_bad_version_is_refused_before_any_fetch():
    with pytest.raises(SchemaSetError, match="is not a version"):
        fetch_set("../1.1.0", fetch=_fetcher({}))


def test_sha256_is_over_ids_and_bytes_in_id_order():
    s = fetch_set(VERSION, fetch=_fetcher(_site()))
    h = hashlib.sha256()
    for sid in sorted(s.files):
        raw = s.files[sid].raw
        h.update(f"{sid}\n{len(raw)}\n".encode() + raw)
    assert s.sha256 == h.hexdigest()
    site = _site()
    site[f"{W3}/B"] += b" "
    assert fetch_set(VERSION, fetch=_fetcher(site)).sha256 != s.sha256


def test_cache_is_filled_once_and_reads_back_the_same_set(tmp_path):
    calls: list[str] = []
    first = load_schema_set(VERSION, cache_dir=tmp_path, fetch=_fetcher(_site(), calls))
    assert len(calls) == 3
    again = load_schema_set(VERSION, cache_dir=tmp_path, fetch=_fetcher({}, calls))
    assert len(calls) == 3
    assert again.sha256 == first.sha256 and again.bundle_id == first.bundle_id
    assert {s: f.url for s, f in again.files.items()} == {s: f.url for s, f in first.files.items()}


def test_a_tampered_set_on_disk_is_refused(tmp_path):
    write_set(fetch_set(VERSION, fetch=_fetcher(_site())), tmp_path)
    target = next(p for p in tmp_path.rglob("B"))
    target.write_bytes(target.read_bytes() + b" ")
    with pytest.raises(SchemaSetError, match="has sha256"):
        read_set(VERSION, tmp_path)


def test_a_set_on_disk_missing_a_file_is_refused(tmp_path):
    write_set(fetch_set(VERSION, fetch=_fetcher(_site())), tmp_path)
    next(p for p in tmp_path.rglob("B")).unlink()
    with pytest.raises(SchemaSetError, match=r"schema .*x/B: can't read"):
        read_set(VERSION, tmp_path)


def test_a_set_on_disk_whose_refs_leave_the_set_is_refused(tmp_path):
    s = fetch_set(VERSION, fetch=_fetcher(_site()))
    write_set(s, tmp_path)
    manifest = json.loads((tmp_path / MANIFEST).read_text())
    manifest["files"] = [e for e in manifest["files"] if not e["id"].endswith("/B")]
    (tmp_path / MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(SchemaSetError, match=r"no schema .*x/B in the set"):
        read_set(VERSION, tmp_path)


# --- the real snapshot -----------------------------------------------------------------------------------------


def test_the_snapshot_is_used_without_any_fetch():
    def no_fetch(url):
        raise AssertionError(f"fetched {url}")

    s = load_schema_set("1.1.0", fetch=no_fetch)
    assert snapshot_dir("1.1.0") is not None
    assert s.bundle["properties"]["schema_version"]["const"] == "1.1.0"
    assert s.bundle_id.endswith("/clinvar-gkm/1.1.0/json/ClinvarGkmBundle")
    assert s.files[s.bundle_id].url == BUNDLE_URL.format(version="1.1.0")
    assert all(f.id == f.url for f in s.files.values() if f.id != s.bundle_id)


def test_the_snapshot_keeps_original_bytes_and_lists_every_file():
    directory = snapshot_dir("1.1.0")
    manifest = json.loads((directory / MANIFEST).read_text())
    listed = {e["file"] for e in manifest["files"]}
    on_disk = {str(p.relative_to(directory)) for p in directory.rglob("*") if p.is_file() and p.name != MANIFEST}
    assert listed == on_disk
    for e in manifest["files"]:
        assert hashlib.sha256((directory / e["file"]).read_bytes()).hexdigest() == e["sha256"]
        assert set(e) == {"id", "url", "file", "sha256"}


def test_vendor_script_refuses_a_bad_version_without_touching_the_snapshot():
    script = Path(__file__).resolve().parents[1] / "scripts" / "vendor-schemas.py"
    run = subprocess.run([sys.executable, str(script), "../x"], capture_output=True, text=True)
    assert run.returncode == 1 and "is not a version" in run.stderr


@pytest.mark.network
def test_fetching_1_1_0_by_url_gives_the_snapshot():
    fetched = fetch_set("1.1.0")
    assert fetched.sha256 == load_schema_set("1.1.0").sha256
