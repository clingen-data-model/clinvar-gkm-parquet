"""replay: check_chain, read_delta, fold and the in-stream replay, through build.load_inputs (the pipeline that
streams the full and applies the deltas' change map, with every section's final state left in DuckDB).

The full is the suite's fixture. There are no real 1.1.0 deltas yet, so the deltas here are SYNTHETIC: built
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

import pytest
from _fixture import SOURCE_RELEASE
from _input_helpers import FIXTURE, bundle_bytes, delta_manifest_json, fixture_sections, local_input, write_gz

from clinvar_gkm_parquet.build import load_inputs
from clinvar_gkm_parquet.bundle import SECTIONS
from clinvar_gkm_parquet.errors import BundleFormatError, ChainError
from clinvar_gkm_parquet.model import BuildInputs, DeltaManifest, InputFile
from clinvar_gkm_parquet.replay import check_chain, check_schema_versions, fold, read_delta
from clinvar_gkm_parquet.sources import load_manifest, resolve_input
from clinvar_gkm_parquet.spec import load_spec

FULL_SOURCE = SOURCE_RELEASE  # the release the fixture's records come from (tests/_fixture.py)
# Two synthetic weekly deltas after it.
D1, D2 = (str(datetime.date.fromisoformat(FULL_SOURCE) + datetime.timedelta(days=d)) for d in (7, 14))
SPEC = load_spec("1.0")  # reads every bundle section


@pytest.fixture(scope="module")
def recs():
    return fixture_sections()


def _delta(tmp_path, release: str, baseline: str, sections: dict, deleted: dict | None = None, **extra):
    """A synthetic delta bundle + manifest on disk: (bundle InputFile, manifest InputFile)."""
    compact = release[:7] + release[8:]
    path = write_gz(tmp_path / "in" / f"clinvar-gkm-delta_{compact}.json.gz", bundle_bytes(sections))
    mpath = tmp_path / "in" / f"clinvar-gkm-delta_{compact}.manifest.json"
    mpath.write_text(json.dumps(delta_manifest_json(release, baseline, deleted, **extra)))
    return local_input(path, release=release), local_input(mpath, release=release)


def _load(tmp_path, deltas=(), full=None):
    inputs = BuildInputs(
        full=full or resolve_input(str(FIXTURE)), full_source_release=FULL_SOURCE, deltas=tuple(deltas)
    )
    return load_inputs(inputs, tmp_path / "work", SPEC)


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


def test_broken_chain_stops_before_any_bundle_is_read(tmp_path, recs):
    d2 = _delta(tmp_path, D2, D1, {"gene": recs["gene"][:1]})
    missing = InputFile(str(tmp_path / "missing.json.gz"), "missing.json.gz", FULL_SOURCE, None, None)
    with pytest.raises(ChainError, match="first delta"):  # not an error about the missing full
        _load(tmp_path, [d2], full=missing)


# ---------------------------------------------------------------------------------------------------------------
# The full alone


def test_full_only(tmp_path, recs):
    state = _load(tmp_path)
    con = state.con
    assert set(state.counts) == set(SECTIONS)
    for section in SECTIONS:
        expected = {_key(k): r.decode() for k, r in recs.get(section, [])}
        assert state.counts[section] == len(expected)
        assert _state(con, section) == expected  # decoded keys, exact record text
    cols = con.execute("DESCRIBE bundle_scv").fetchall()
    assert [(c[0], c[1]) for c in cols] == [
        ("key", "VARCHAR"),
        ("record", "VARCHAR"),
        ("ordinal", "BIGINT"),  # the record's position in its section (model.py staging format)
        ("key_json", "VARCHAR"),  # the key as written
    ]
    for section in SECTIONS:
        got = con.execute(f'SELECT key_json, ordinal FROM "bundle_{section}" ORDER BY ordinal').fetchall()
        assert got == [(k.decode(), i) for i, (k, _) in enumerate(recs.get(section, []))]
    assert not list((tmp_path / "work" / "stage").glob("**/*.tsv.gz"))


def test_escaped_keys_are_decoded(tmp_path, recs):
    key, record = recs["gene"][0]
    odd = b'"synthetic \\"q\\" \\\\ caf\\u00e9 \xc3\xbc \\t"'
    path = write_gz(tmp_path / "b.json.gz", bundle_bytes({"gene": [(odd, record), (key, record)]}))
    state = _load(tmp_path, full=local_input(path))
    assert set(_state(state.con, "gene")) == {'synthetic "q" \\ café ü \t', _key(key)}


def test_duplicate_key_in_full(tmp_path, recs):
    gene = recs["gene"]
    path = write_gz(tmp_path / "dup.json.gz", bundle_bytes({"gene": [gene[0], gene[1], gene[0]]}))
    with pytest.raises(BundleFormatError, match=f"dup.json.gz: section gene, key {_key(gene[0][0])}: appears 2"):
        _load(tmp_path, full=local_input(path))


def test_duplicate_key_in_full_that_a_delta_touches(tmp_path, recs):
    # A key the change map holds is dropped from the stream, so its repeat is caught there, not in DuckDB.
    gene = recs["gene"]
    path = write_gz(tmp_path / "dup.json.gz", bundle_bytes({"gene": [gene[0], gene[1], gene[0]]}))
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": [(gene[0][0], gene[1][1])]})
    with pytest.raises(
        BundleFormatError, match=f"dup.json.gz: section gene, key {_key(gene[0][0])}: appears more"
    ):
        _load(tmp_path, [d1], full=local_input(path))


# ---------------------------------------------------------------------------------------------------------------
# Synthetic deltas over the full


def test_update_add_delete(tmp_path, recs):
    scv = recs["scv"]
    (k0, r0), (k1, r1), (k2, _) = scv[0], scv[1], scv[2]
    new_key = b'"synthetic:SCV-new"'
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"scv": [(k0, r1), (new_key, r0)]}, deleted={"scv": [_key(k2)]})
    state = _load(tmp_path, [d1])
    scv_state = _state(state.con, "scv")
    assert scv_state[_key(k0)] == r1.decode()  # updated with another real record's bytes
    assert scv_state[_key(new_key)] == r0.decode()  # added
    assert _key(k2) not in scv_state  # deleted
    assert scv_state[_key(k1)] == r1.decode()  # untouched
    assert state.counts["scv"] == len(scv)  # +1 added, -1 deleted
    # other sections are the full as is
    assert _state(state.con, "rcv") == {_key(k): r.decode() for k, r in recs["rcv"]}


def test_highest_seq_wins(tmp_path, recs):
    (k0, r0), (_, r1), (_, r2) = recs["vcv"][:3]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"vcv": [(k0, r1)]})
    d2 = _delta(tmp_path, D2, D1, {"vcv": [(k0, r2)]})
    assert _state(_load(tmp_path, [d1, d2]).con, "vcv")[_key(k0)] == r2.decode()


def test_delete_then_add_in_a_later_delta(tmp_path, recs):
    (k0, r0), (_, r1) = recs["rcv"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {}, deleted={"rcv": [_key(k0)]})
    d2 = _delta(tmp_path, D2, D1, {"rcv": [(k0, r1)]})
    assert _key(k0) not in _state(_load(tmp_path / "a", [d1]).con, "rcv")
    assert _state(_load(tmp_path / "b", [d1, d2]).con, "rcv")[_key(k0)] == r1.decode()


def test_add_then_delete_in_a_later_delta(tmp_path, recs):
    (k0, r0) = recs["condition"][0]
    new_key = b'"synthetic:trait-new"'
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"condition": [(new_key, r0)]})
    d2 = _delta(tmp_path, D2, D1, {}, deleted={"condition": [_key(new_key)]})
    assert _key(new_key) not in _state(_load(tmp_path, [d1, d2]).con, "condition")


def test_upsert_beats_delete_in_the_same_delta(tmp_path, recs, caplog):
    (k0, _), (_, r1) = recs["evidenceLine"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"evidenceLine": [(k0, r1)]}, deleted={"evidenceLine": [_key(k0)]})
    assert _state(_load(tmp_path, [d1]).con, "evidenceLine")[_key(k0)] == r1.decode()
    assert "not in the prior state" not in caplog.text


def test_delete_of_a_missing_key_warns(tmp_path, recs, caplog):
    (k0, _) = recs["submitter"][0]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {}, deleted={"submitter": [_key(k0), "synthetic:absent"]})
    d2 = _delta(tmp_path, D2, D1, {}, deleted={"submitter": [_key(k0)]})  # already gone
    with caplog.at_level(logging.WARNING, logger="clinvar_gkm_parquet.replay"):
        state = _load(tmp_path, [d1, d2])
    assert state.counts["submitter"] == len(recs["submitter"]) - 1
    msgs = [r.getMessage() for r in caplog.records if r.name == "clinvar_gkm_parquet.replay"]
    assert any(f"delta {D1}: 1 deleted submitter key(s)" in m and "synthetic:absent" in m for m in msgs)
    assert any(f"delta {D2}: 1 deleted submitter key(s)" in m and _key(k0) in m for m in msgs)
    assert len(msgs) == 2


def test_delete_after_an_upsert_of_an_absent_key_is_not_a_warning(tmp_path, recs, caplog):
    (_, r0) = recs["gene"][0]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": [(b'"synthetic:gene-new"', r0)]})
    d2 = _delta(tmp_path, D2, D1, {}, deleted={"gene": ["synthetic:gene-new"]})
    with caplog.at_level(logging.WARNING, logger="clinvar_gkm_parquet.replay"):
        state = _load(tmp_path, [d1, d2])
    assert "synthetic:gene-new" not in _state(state.con, "gene")
    assert "not in the prior state" not in caplog.text


def test_delta_into_a_section_absent_from_the_full(tmp_path, recs):
    absent = [s for s in SECTIONS if s not in recs]
    if not absent:
        pytest.skip("the fixture has every section")
    section = absent[0]
    (_, r0) = next(iter(v for v in recs.values() if v))[0]  # any real record's bytes (replay doesn't parse them)
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {section: [(b'"synthetic:new-in-delta"', r0)]})
    state = _load(tmp_path, [d1])
    assert state.counts[section] == 1
    assert _state(state.con, section) == {"synthetic:new-in-delta": r0.decode()}


def test_a_delta_can_empty_a_section(tmp_path, recs):
    section = min((s for s in recs if recs[s]), key=lambda s: len(recs[s]))  # the fixture's smallest section
    keys = [_key(k) for k, _ in recs[section]]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {}, deleted={section: keys})
    state = _load(tmp_path, [d1])
    assert state.counts[section] == 0 and _state(state.con, section) == {}


def test_duplicate_key_in_a_delta(tmp_path, recs):
    (k0, r0), (_, r1) = recs["gene"][:2]
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": [(k0, r0), (k0, r1)]})
    name = f"clinvar-gkm-delta_{D1[:7]}{D1[8:]}.json.gz"
    with pytest.raises(BundleFormatError, match=f"{name}: section gene, key {_key(k0)}: appears 2 times"):
        _load(tmp_path, [d1])


def test_schema_versions_must_match(tmp_path, recs):
    b, m = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]})
    man = load_manifest(m)
    delta = read_delta(b, man)
    check_schema_versions("full.json.gz", "1.1.0", [(delta, man)])
    other = dataclasses.replace(delta, streamed=dataclasses.replace(delta.streamed, schema_version="1.1.1"))
    with pytest.raises(ChainError, match="same one"):
        check_schema_versions("full.json.gz", "1.1.0", [(other, man)])


def test_delta_must_match_its_manifest_sha256(tmp_path, recs):
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]}, sha256="0" * 64)
    with pytest.raises(ChainError, match="differs from the one its manifest"):
        _load(tmp_path, [d1])


def test_delta_paired_with_the_wrong_manifest(tmp_path, recs):
    d1 = _delta(tmp_path, D1, FULL_SOURCE, {"gene": recs["gene"][:1]})
    d2 = _delta(tmp_path, D2, D1, {"gene": recs["gene"][1:2]})
    with pytest.raises(ChainError, match=f"paired with the manifest for {D2}"):
        read_delta(d1[0], load_manifest(d2[1]))


def test_fold_order_and_tombstones(tmp_path, recs):
    """The change map without the full: the last delta to touch a key wins, deletes before upserts."""
    (k0, r0), (k1, r1) = recs["gene"][:2]
    m1 = load_manifest(_delta(tmp_path, D1, FULL_SOURCE, {}, deleted={"gene": [_key(k0)]})[1])
    m2 = load_manifest(_delta(tmp_path, D2, D1, {}, deleted={"gene": [_key(k1)]})[1])
    b1 = read_delta(_delta(tmp_path, D1, FULL_SOURCE, {"gene": [(k1, r0)]}, deleted={"gene": [_key(k0)]})[0], m1)
    b2 = read_delta(_delta(tmp_path, D2, D1, {"gene": [(k0, r1)]}, deleted={"gene": [_key(k1)]})[0], m2)
    ch = fold([(b1, m1), (b2, m2)])["gene"]
    assert ch.final == {_key(k0).encode(): (k0, r1), _key(k1).encode(): None}
    assert ch.full_deletes == {_key(k0).encode(): [1]}  # D2's delete of k1 follows D1's upsert of it
    assert ch.absent_deletes == {}
