"""End to end with the real spec and table SQL: stage, replay, build, self-check, verify and recompute, on the
example fixture moved to the current 1.0 shape (current_shape(), see test_tables.py). The raw fixture's run is
tests/test_build.py::test_build_fixture_end_to_end, a strict xfail for the fixture's older shape."""

from __future__ import annotations

import hashlib

from _tables_helpers import SPEC, current_shape, read_fixture, write_bundle

from clinvar_gkm_parquet import build as build_mod
from clinvar_gkm_parquet.build import build
from clinvar_gkm_parquet.model import BuildInputs, InputFile
from clinvar_gkm_parquet.verify import recompute, recompute_failed, verify_dir


def _inputs(path):
    inp = InputFile(
        source=str(path),
        name=path.name,
        release="2026-10-18",
        expected_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        sha256_source="explicit",
        size=path.stat().st_size,
    )
    return BuildInputs(full=inp, full_source_release="2026-10-18")


def test_build_current_shape_fixture_end_to_end(tmp_path):
    sections = current_shape(read_fixture())
    bundle = write_bundle(sections, tmp_path / "clinvar-gkm_current-shape.json.gz")
    a = build(_inputs(bundle), tmp_path / "a", spec=SPEC, threads=1)  # clinvar-gkm: build()'s default is baseline
    b = build(_inputs(bundle), tmp_path / "b", spec=SPEC, threads=4)
    assert [(t.table, t.sha256) for t in a.tables] == [(t.table, t.sha256) for t in b.tables]
    rows = {t.table: t.rows for t in a.tables}
    assert set(rows) == {t.name for t in SPEC.tables}
    assert sum(rows.values()) == sum(len(v) for v in sections.values())
    assert verify_dir(tmp_path / "a") == []
    state = build_mod.load_inputs(_inputs(bundle), tmp_path / "rc", SPEC)
    try:
        groups = recompute(state.con, SPEC, tmp_path / "a")
        assert groups and not any(recompute_failed(g) for g in groups)
    finally:
        state.con.close()
