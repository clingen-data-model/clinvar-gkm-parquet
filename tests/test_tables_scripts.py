"""scripts/render-spec-readme.py (the committed README is up to date) and scripts/capture-spec.py (on Parquet files
built here from the fixture, as the real 1.1.0 export will be read)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml
from _tables_helpers import SPEC, build, connect, current_shape, read_fixture

from clinvar_gkm_parquet.spec import parse_spec, spec_dir

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


render_readme = _load("render-spec-readme")
capture_spec = _load("capture-spec")


def test_spec_readme_is_up_to_date():
    readme = spec_dir("1.0") / "README.md"
    assert readme.read_text(encoding="utf-8") == render_readme.render("1.0"), (
        "spec/1.0/README.md is stale: run scripts/render-spec-readme.py"
    )


def test_readme_render_is_deterministic():
    assert render_readme.render("1.0") == render_readme.render("1.0")


@pytest.fixture(scope="module")
def con():
    c = connect(current_shape(read_fixture()))
    for t in SPEC.tables:
        build(c, t)
    return c


def _export(con, out: Path, overrides: dict[str, str] | None = None):
    """Write every built table to <out>/<table>.parquet; overrides[table] replaces its SELECT."""
    out.mkdir(parents=True, exist_ok=True)
    for t in SPEC.tables:
        sql = (overrides or {}).get(t.name, f'SELECT * FROM "out_{t.name}"')
        con.execute(f"COPY ({sql}) TO '{out / t.file_name}' (FORMAT parquet)")
    return out


def test_capture_matches_the_spec(con, tmp_path):
    captured = capture_spec.capture(_export(con, tmp_path / "pq"), SPEC)
    problems, notes = capture_spec.diff(SPEC, captured)
    assert problems == []
    assert set(captured) == {t.name for t in SPEC.tables}
    # The fixture shows these (name -> slot) pairs, and the scan finds them where they are.
    slots = captured["scv"]["slots"]["extensions"]
    assert slots[("[]", "submittedCondition")] == {"value_submitted_condition"}
    assert slots[("[]", "clinvarScvId")] == {"value_string"}
    assert captured["variation"]["slots"]["extensions"][("[]", "clinvarGeneList")] == {"value_gene_array"}
    # A name the spec lists but the fixture lacks is a note, not a difference (both fixtures lack some).
    assert any("not seen" in n for n in notes)


def test_capture_reports_differences(con, tmp_path):
    overrides = {
        # a type change
        "copyNumberCount": 'SELECT * REPLACE (CAST(copies AS INTEGER) AS copies) FROM "out_copyNumberCount"',
        # a missing column
        "gene": 'SELECT * EXCLUDE (name) FROM "out_gene"',
        # an extension name the slot map doesn't list
        "variation": 'SELECT * REPLACE (list_transform(extensions, lambda e: struct_update(e, "name" := '
        "e.name || 'X')) AS extensions) FROM \"out_variation\"",
    }
    out = _export(con, tmp_path / "pq", overrides)
    (out / "extra.parquet").write_bytes((out / "gene.parquet").read_bytes())
    (out / "submitter.parquet").unlink()
    problems, _ = capture_spec.diff(SPEC, capture_spec.capture(out, SPEC))
    text = "\n".join(problems)
    assert "copyNumberCount.copies: type" in text and "file: INTEGER" in text
    assert "gene.name: column missing from the file" in text
    assert "extension 'clinvarGeneListX'" in text and "not in the spec's slot map" in text
    assert "extra: file not in the spec" in text
    assert "submitter: no submitter.parquet" in text


def test_capture_write_puts_captured_types_in_a_copy(con, tmp_path):
    overrides = {
        "copyNumberCount": 'SELECT * REPLACE (CAST(copies AS INTEGER) AS copies) FROM "out_copyNumberCount"'
    }
    captured = capture_spec.capture(_export(con, tmp_path / "pq", overrides), SPEC)
    out = tmp_path / "tables.yaml"
    n = capture_spec.write_types(spec_dir("1.0") / "tables.yaml", captured, out)
    assert n == 1
    written = parse_spec(yaml.safe_load(out.read_text(encoding="utf-8")))
    assert written.table("copyNumberCount").column("copies").type == "INTEGER"
    assert capture_spec.diff(written, captured)[0] == []


def test_capture_cli_exit_codes(con, tmp_path, capsys):
    out = _export(con, tmp_path / "pq")
    assert capture_spec.main([str(out)]) == 0
    (out / "gene.parquet").unlink()
    assert capture_spec.main([str(out)]) == 1
    assert "gene: no gene.parquet" in capsys.readouterr().out
