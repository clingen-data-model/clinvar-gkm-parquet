import json

from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.compat import TABLE_SPEC_VERSION


def test_version_json(capsys):
    assert main(["version", "--json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["table_spec_version"] == TABLE_SPEC_VERSION
    assert info["snapshotted_schema_versions"] == ["1.1.0"]
    assert "sections compiles any bundle from its schema and column set" in info["schema_version_rule"]
    assert "accepted_schema_versions" not in info  # it can't state a closed list any more (ADR 0006 §6)
    assert info["duckdb_version"] == "1.5.4"
    assert info["tables"]
    assert info["default_table_spec"] == "sections"
    specs = {s["name"]: s for s in info["table_specs"]}
    assert set(specs) == {"sections", "baseline", "clinvar-gkm"} and specs["sections"]["default"]
    assert specs["sections"]["compiled"] and not specs["baseline"]["compiled"]
    assert len(specs["sections"]["tables_by_schema_version"]["1.1.0"]) == len(specs["baseline"]["tables"]) == 20
    assert len(specs["clinvar-gkm"]["tables"]) == 22


def test_version_text(capsys):
    assert main(["version"]) == 0
    out = capsys.readouterr().out
    assert "built-in table specs sections 1.0 (draft, default), baseline 1.0 (" in out and "clinvar-gkm 1.0" in out
