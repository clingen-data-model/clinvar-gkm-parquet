"""replay.check_chain and replay.load_state.

The full is the example fixture. There are no real 1.1.0 deltas yet, so the deltas here are SYNTHETIC: built
in the bundle layout from the fixture's real records (an update is one key carrying another real record's
bytes), with synthetic manifests in build-delta-manifest.py's shape.
TODO: replace with real replay fixtures from published 1.1.0 deltas (design plan §8.1: keys added, updated
and deleted in two consecutive deltas' manifests).
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import logging

import duckdb
import pytest
from _fixture import SOURCE_RELEASE
from _input_helpers import FIXTURE, bundle_bytes, delta_manifest_json, fixture_sections, local_input, write_gz

from clinvar_gkm_parquet.bundle import SECTIONS, stage_bundle
from clinvar_gkm_parquet.errors import BundleFormatError, ChainError
from clinvar_gkm_parquet.model import DeltaManifest
from clinvar_gkm_parquet.replay import check_chain, load_state
from clinvar_gkm_parquet.sources import load_manifest, resolve_input

FULL_SOURCE = SOURCE_RELEASE  # the release the fixture's records come from (tests/_fixture.py)
# Two synthetic weekly deltas after it.
D1, D2 = (str(datetime.date.fromisoformat(FULL_SOURCE) + datetime.timedelta(days=d)) for d in (7, 14))


@pytest.fixture(scope="module")
def recs():
    return fixture_sections()


@pytest.fixture
def full(tmp_path):
    return stage_bundle(resolve_input(str(FIXTURE)), tmp_path / "work")


def _delta(tmp_path, release: str, baseline: str, sections: dict, deleted: dict | None = None, **extra):
    """A synthetic delta bundle + manifest, staged / loaded."""
    compact = release[:7] + release[8:]
    path = write_gz(tmp_path / "in" / f"clinvar-gkm-delta_{compact}.json.gz", bundle_bytes(sections))
    staged = stage_bundle(local_input(path, release=release), tmp_path / "work")
    mpath = tmp_path / "in" / f"clinvar-gkm-delta_{compact}.manifest.json"
    mpath.write_text(json.dumps(delta_manifest_json(release, baseline, deleted, **extra)))
    return staged, load_manifest(local_input(mpath, release=release))


def _state(con, section: str) -> dict[str, str]:
    return dict(con.execute(f'SELECT key, record FROM "bundle_{section}"').fetchall())


def _key(k: bytes) -> str:
    return json.loads(k)


# ---------------------------------------------------------------------------------------------------------------
# check_chain


def _man(release, baseline):
    return DeltaManifest(release=release, baseline_release=baseline, deleted={}, checkpoint_full=None)


def test_chain_ok():
    check_chain("2026-09-28", [_man("2026-10-04", "2026-09-28"), _man("2026-10-11", "2026-10-04")])
    check_chain(None, [])


def test_chain_first_delta_must_follow_the_full():
    with pytest.raises(ChainError, match="first delta 2026-10-11 has baseline_release 2026-10-04"):
        check_chain("2026-09-28", [_man("2026-10-11", "2026-10-04")])


def test_chain_gap():
    with pytest.raises(ChainError, match="delta 2026-10-18 has baseline_release 2026-10-11.*delta 2026-10-04"):
        check_chain("2026-09-28", [_man("2026-10-04", "2026-09-28"), _man("2026-10-18", "2026-10-11")])


def test_chain_out_of_order():
    with pytest.raises(ChainError):
        check_chain("2026-09-28", [_man("2026-10-11", "2026-10-04"), _man("2026-10-04", "2026-09-28")])


def test_chain_needs_full_source_release():
    with pytest.raises(ChainError, match="source release is unknown"):
        check_chain(None, [_man("2026-10-04", "2026-09-28")])


def test_chain_null_baseline():
    with pytest.raises(ChainError):
        check_chain("2026-09-28", [_man("2026-10-04", None)])


def test_chain_not_later():
    with pytest.raises(ChainError, match="not later"):
        check_chain("2026-10-04", [_man("2026-10-04", "2026-10-04")])


# ---------------------------------------------------------------------------------------------------------------
# load_state: the full alone


def test_full_only(full, recs):
    con = duckdb.connect()
    counts = load_state(con, full, [])
    assert set(counts) == set(SECTIONS)
    for section in SECTIONS:
        expected = {_key(k): r.decode() for k, r in recs.get(section, [])}
        assert counts[section] == len(expected)
        assert _state(con, section) == expected  # decoded keys, exact record text
    cols = con.execute("DESCRIBE bundle_scv").fetchall()
    assert [(c[0], c[1]) for c in cols] == [("key", "VARCHAR"), ("record", "VARCHAR")]


def test_escaped_keys_are_decoded(tmp_path, recs):
    key, record = recs["gene"][0]
    odd = b'"synthetic \\"q\\" \\\\ caf\\u00e9 \xc3\xbc \\t"'
    path = write_gz(tmp_path / "b.json.gz", bundle_bytes({"gene": [(odd, record), (key, record)]}))
    staged = stage_bundle(local_input(path), tmp_path / "work")
    con = duckdb.connect()
    load_state(con, staged, [])
    assert set(_state(con, "gene")) == {'synthetic "q" \\ café ü \t', _key(key)}


def test_duplicate_key_in_full(tmp_path, recs):
    gene = recs["gene"]
    path = write_gz(tmp_path / "dup.json.gz", bundle_bytes({"gene": [gene[0], gene[1], gene[0]]}))
    staged = stage_bundle(local_input(path), tmp_path / "work")
    with pytest.raises(BundleFormatError, match=f"dup.json.gz: section gene, key {_key(gene[0][0])}: appears 2"):
        load_state(duckdb.connect(), staged, [])


# ---------------------------------------------------------------------------------------------------------------
# load_state: synthetic deltas over the full


def test_update_add_delete(tmp_path, full, recs):
    scv = recs["scv"]
    (k0, r0), (k1, r1), (k2, _) = scv[0], scv[1], scv[2]
    new_key = b'"synthetic:SCV-new"'
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"scv": [(k0, r1), (new_key, r0)]}, deleted={"scv": [_key(k2)]})
    con = duckdb.connect()
    check_chain(FULL_SOURCE, [d1[1]])
    counts = load_state(con, full, [d1])
    state = _state(con, "scv")
    assert state[_key(k0)] == r1.decode()  # updated with another real record's bytes
    assert state[_key(new_key)] == r0.decode()  # added
    assert _key(k2) not in state  # deleted
    assert state[_key(k1)] == r1.decode()  # untouched
    assert counts["scv"] == len(scv)  # +1 added, -1 deleted
    # other sections are the full as is
    assert _state(con, "rcv") == {_key(k): r.decode() for k, r in recs["rcv"]}


def test_highest_seq_wins(tmp_path, full, recs):
    (k0, r0), (_, r1), (_, r2) = recs["vcv"][:3]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"vcv": [(k0, r1)]})
    d2 = _delta(tmp_path, D2, D1, {"vcv": [(k0, r2)]})
    con = duckdb.connect()
    load_state(con, full, [d1, d2])
    assert _state(con, "vcv")[_key(k0)] == r2.decode()


def test_delete_then_add_in_a_later_delta(tmp_path, full, recs):
    (k0, r0), (_, r1) = recs["rcv"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {}, deleted={"rcv": [_key(k0)]})
    d2 = _delta(tmp_path, D2, D1, {"rcv": [(k0, r1)]})
    con = duckdb.connect()
    load_state(con, full, [d1])
    assert _key(k0) not in _state(con, "rcv")
    load_state(con, full, [d1, d2])
    assert _state(con, "rcv")[_key(k0)] == r1.decode()


def test_add_then_delete_in_a_later_delta(tmp_path, full, recs):
    (k0, r0) = recs["condition"][0]
    new_key = b'"synthetic:trait-new"'
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"condition": [(new_key, r0)]})
    d2 = _delta(tmp_path, D2, D1, {}, deleted={"condition": [_key(new_key)]})
    con = duckdb.connect()
    load_state(con, full, [d1, d2])
    assert _key(new_key) not in _state(con, "condition")


def test_upsert_beats_delete_in_the_same_delta(tmp_path, full, recs, caplog):
    (k0, _), (_, r1) = recs["evidenceLine"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"evidenceLine": [(k0, r1)]}, deleted={"evidenceLine": [_key(k0)]})
    con = duckdb.connect()
    load_state(con, full, [d1])
    assert _state(con, "evidenceLine")[_key(k0)] == r1.decode()
    assert "not in the prior state" not in caplog.text


def test_delete_of_a_missing_key_warns(tmp_path, full, recs, caplog):
    (k0, _) = recs["submitter"][0]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {}, deleted={"submitter": [_key(k0), "synthetic:absent"]})
    d2 = _delta(tmp_path, D2, D1, {}, deleted={"submitter": [_key(k0)]})  # already gone
    con = duckdb.connect()
    with caplog.at_level(logging.WARNING, logger="clinvar_gkm_parquet.replay"):
        counts = load_state(con, full, [d1, d2])
    assert counts["submitter"] == len(recs["submitter"]) - 1
    msgs = [r.getMessage() for r in caplog.records]
    assert any(f"delta {D1}: 1 deleted submitter key(s)" in m and "synthetic:absent" in m for m in msgs)
    assert any(f"delta {D2}: 1 deleted submitter key(s)" in m and _key(k0) in m for m in msgs)


def test_delta_into_a_section_absent_from_the_full(tmp_path, full, recs):
    absent = [s for s in SECTIONS if s not in full.sections]
    if not absent:
        pytest.skip("the fixture has every section")
    section = absent[0]
    (_, r0) = next(iter(v for v in recs.values() if v))[0]  # any real record's bytes (replay doesn't parse them)
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {section: [(b'"synthetic:new-in-delta"', r0)]})
    con = duckdb.connect()
    counts = load_state(con, full, [d1])
    assert counts[section] == 1
    assert _state(con, section) == {"synthetic:new-in-delta": r0.decode()}


def test_duplicate_key_in_a_delta(tmp_path, full, recs):
    (k0, r0), (_, r1) = recs["gene"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": [(k0, r0), (k0, r1)]})
    with pytest.raises(BundleFormatError, match=f"clinvar-gkm-delta_{D1[:7]}{D1[8:]}.json.gz: section gene"):
        load_state(duckdb.connect(), full, [d1])


def test_schema_versions_must_match(tmp_path, full, recs):
    staged, man = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]})
    other = dataclasses.replace(staged, schema_version="1.1.1")
    with pytest.raises(ChainError, match="same one"):
        load_state(duckdb.connect(), full, [(other, man)])


def test_delta_must_match_its_manifest_sha256(tmp_path, full, recs):
    staged, man = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]}, sha256="0" * 64)
    with pytest.raises(ChainError, match="differs from the one its manifest"):
        load_state(duckdb.connect(), full, [(staged, man)])


def test_delta_paired_with_the_wrong_manifest(tmp_path, full, recs):
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]})
    d2 = _delta(tmp_path, D2, D1, {"gene": recs["gene"][1:2]})
    with pytest.raises(ChainError, match=f"paired with the manifest for {D2}"):
        load_state(duckdb.connect(), full, [(d1[0], d2[1])])
