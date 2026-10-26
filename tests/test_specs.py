"""Spec names, the built-in registry, custom spec files and their rules, KV metadata, version --json (ADR 0003)."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest
import yaml

from clinvar_gkm_parquet import bundle, compat
from clinvar_gkm_parquet.errors import SpecError
from clinvar_gkm_parquet.spec import (
    SPEC_KV_KEYS,
    kv_metadata,
    load_spec,
    resolve_spec,
    spec_dir,
    spec_from_kv,
)

ROOT = Path(__file__).resolve().parents[1]
BASELINE_FILE = ROOT / "spec" / "baseline" / "1.0" / "tables.yaml"
CLINVAR_GKM_FILE = ROOT / "spec" / "1.0" / "tables.yaml"


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


# --- built-ins -------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "arg, name, path",
    [
        (None, "baseline", BASELINE_FILE),
        ("baseline", "baseline", BASELINE_FILE),
        ("baseline-1.0", "baseline", BASELINE_FILE),
        ("baseline@1.0", "baseline", BASELINE_FILE),
        ("clinvar-gkm", "clinvar-gkm", CLINVAR_GKM_FILE),
        ("clinvar-gkm-1.0", "clinvar-gkm", CLINVAR_GKM_FILE),
        ("clinvar-gkm@1.0", "clinvar-gkm", CLINVAR_GKM_FILE),
    ],
)
def test_resolve_builtin(arg, name, path):
    spec, src = resolve_spec(arg)
    assert (src.name, src.version, src.builtin) == (name, "1.0", True)
    assert (spec.name, spec.spec_version) == (name, "1.0")
    assert src.path.resolve() == path.resolve()
    assert src.sha256 == _sha(path)


def test_builtins_match_compat():
    assert compat.DEFAULT_SPEC == "baseline"
    for name, version in compat.BUILTIN_SPECS.items():
        spec, src = resolve_spec(name)
        assert (spec.name, spec.spec_version, src.version) == (name, version, version)
        assert tuple(spec.bundle_schema_versions) == compat.ACCEPTED_SCHEMA_VERSIONS
    assert resolve_spec("clinvar-gkm")[0] is load_spec("1.0")  # the old entry point is the clinvar-gkm spec
    assert spec_dir("1.0") == CLINVAR_GKM_FILE.parent


def test_baseline_reads_every_section():
    spec, _ = resolve_spec(None)
    assert spec.sections == set(compat.SCHEMA_SECTIONS["1.1.0"])


def test_schema_sections_match_the_reader():
    assert compat.SCHEMA_SECTIONS["1.1.0"] == bundle.SECTIONS


@pytest.mark.parametrize("arg", ["clinvar-gkm@2.0", "baseline-9.9"])
def test_unknown_builtin_version(arg):
    with pytest.raises(SpecError, match="no built-in table spec"):
        resolve_spec(arg)


def test_not_a_builtin_nor_a_file(tmp_path):
    with pytest.raises(SpecError, match="not a built-in table spec"):
        resolve_spec(str(tmp_path / "missing.yaml"))
    with pytest.raises(SpecError, match="not a built-in table spec"):
        resolve_spec("clinvar")


def test_baseline_file_is_generated():
    spec_ = importlib.util.spec_from_file_location(
        "make_baseline_spec", ROOT / "scripts" / "make-baseline-spec.py"
    )
    mod = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(mod)
    assert mod.main(["--check"]) == 0


def test_wheel_includes_the_spec_tree():
    """The wheel force-includes spec/ as clinvar_gkm_parquet/_spec, so builtin_dir finds both built-ins there."""
    tomllib = pytest.importorskip("tomllib")  # Python 3.11+
    py = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert py["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]["spec"] == "clinvar_gkm_parquet/_spec"
    assert BASELINE_FILE.is_file() and CLINVAR_GKM_FILE.is_file()


# --- custom files ----------------------------------------------------------------------------------------------


def _baseline_raw() -> dict:
    return yaml.safe_load(BASELINE_FILE.read_text(encoding="utf-8"))


def _write(tmp_path: Path, raw: dict, name: str = "tables.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return p


def _custom(**top) -> dict:
    raw = _baseline_raw()
    raw["name"] = "my-genes"
    raw["spec_version"] = "0.1"
    raw["tables"] = [t for t in raw["tables"] if t["name"] in ("gene", "scv")]
    raw.update(top)
    return raw


def test_custom_file_passes(tmp_path):
    p = _write(tmp_path, _custom())
    spec, src = resolve_spec(str(p))
    assert (src.name, src.version, src.builtin, src.path) == ("my-genes", "0.1", False, p)
    assert src.sha256 == _sha(p)
    assert [t.name for t in spec.tables] == ["gene", "scv"]


def test_custom_file_with_a_template_table_passes(tmp_path):
    """A custom table that isn't key/refs/record must have a SQL template: a clinvar-gkm table qualifies."""
    raw = yaml.safe_load(CLINVAR_GKM_FILE.read_text(encoding="utf-8"))
    raw["name"] = "allele-only"
    raw["tables"] = [t for t in raw["tables"] if t["name"] == "allele"]
    spec, src = resolve_spec(str(_write(tmp_path, raw)))
    assert [t.name for t in spec.tables] == ["allele"] and not src.builtin


def test_custom_sort_key_with_documented_uniqueness(tmp_path):
    raw = _custom()
    raw["tables"][0]["sort_key"] = "data"
    with pytest.raises(SpecError, match="not the key column"):
        resolve_spec(str(_write(tmp_path, raw)))
    raw["tables"][0]["sort_key_unique"] = "data holds the key (gene records carry their id)"
    spec, _ = resolve_spec(str(_write(tmp_path, raw, "ok.yaml")))
    assert spec.table("gene").sort_key_unique


def _drop_kind(raw: dict, kind: str) -> dict:
    t = raw["tables"][0]
    t["columns"] = [c for c in t["columns"] if c["derivation"]["kind"] != kind]
    return raw


def _bad_cases() -> list[tuple[str, callable, str]]:
    def unknown_top(r):
        r["surprise"] = 1
        return r

    def unknown_table_key(r):
        r["tables"][0]["surprise"] = 1
        return r

    def unknown_column_key(r):
        r["tables"][0]["columns"][0]["surprise"] = 1
        return r

    def unknown_section(r):
        r["tables"][0]["sections"] = ["genes"]
        return r

    def unknown_kind(r):
        r["tables"][0]["columns"][1]["derivation"]["kind"] = "pointers"
        return r

    def no_name(r):
        del r["name"]
        return r

    def bad_name(r):
        r["name"] = "My Spec"
        return r

    def builtin_name(r):
        r["name"] = "clinvar-gkm"
        return r

    def unaccepted_schema(r):
        r["bundle_schema_versions"] = ["1.0.1"]
        return r

    def no_key(r):
        r["tables"][0]["sort_key"] = "data"
        r["tables"][0]["sort_key_unique"] = "test"
        return _drop_kind(r, "key")

    def no_renderer(r):  # a computed column in a table the tool has no SQL template for
        r["tables"][0]["name"] = "gene_names"
        r["tables"][0]["columns"][1]["derivation"] = {"kind": "computed", "path": "$.name", "transform": "upper"}
        r["tables"][0]["columns"][1]["type"] = "VARCHAR"
        return r

    def two_keys(r):
        c = dict(r["tables"][0]["columns"][0], name="id2")
        r["tables"][0]["columns"].append(c)
        return r

    def bad_writer(r):
        r["writer"] = {"compression": "snappy", "row_group_size": "big"}
        return r

    def nullable_key(r):
        r["tables"][0]["columns"][0]["nulls"] = "when-absent"
        return r

    return [
        ("unknown top-level key", unknown_top, "unknown key"),
        ("unknown table key", unknown_table_key, "unknown key"),
        ("unknown column key", unknown_column_key, "unknown key"),
        ("unknown section", unknown_section, "not sections of bundle schema 1.1.0"),
        ("unknown derivation kind", unknown_kind, "derivation kind must be one of"),
        ("no key column", no_key, "exactly one key column"),
        ("no record column", lambda r: _drop_kind(r, "record"), "exactly one record column"),
        ("two key columns", two_keys, "exactly one key column"),
        ("nullable key", nullable_key, "must have nulls: never"),
        ("no name", no_name, "missing key"),
        ("bad name", bad_name, "must be lower case"),
        ("a built-in's name", builtin_name, "built-in spec's"),
        ("unaccepted schema_version", unaccepted_schema, "not read by this tool"),
        ("no renderer", no_renderer, "no renderer"),
        ("bad writer", bad_writer, "row_group_size"),
    ]


@pytest.mark.parametrize("label, mutate, match", _bad_cases(), ids=[c[0] for c in _bad_cases()])
def test_bad_custom_file_fails(tmp_path, label, mutate, match):
    p = _write(tmp_path, mutate(_custom()))
    with pytest.raises(SpecError, match=match):
        resolve_spec(str(p))


def test_not_yaml(tmp_path):
    p = tmp_path / "x.yaml"
    p.write_text("name: [unclosed", encoding="utf-8")
    with pytest.raises(SpecError, match="not a YAML file"):
        resolve_spec(str(p))


# --- key-value metadata ----------------------------------------------------------------------------------------


def test_kv_metadata_builtin_and_custom(tmp_path):
    _, base = resolve_spec(None)
    assert kv_metadata(base) == {"table_spec": "baseline", "table_spec_version": "1.0"}
    p = _write(tmp_path, _custom())
    _, custom = resolve_spec(str(p))
    kv = kv_metadata(custom)
    assert kv == {
        "table_spec": "my-genes",
        "table_spec_version": "0.1",
        "table_spec_builtin": "false",
        "table_spec_sha256": _sha(p),
    }
    assert tuple(kv) == SPEC_KV_KEYS


def test_spec_from_kv(tmp_path):
    assert spec_from_kv({"table_spec_version": "1.0"})[1].name == "clinvar-gkm"  # written before specs had names
    assert spec_from_kv({"table_spec": "baseline", "table_spec_version": "1.0"})[1].name == "baseline"
    p = _write(tmp_path, _custom())
    kv = kv_metadata(resolve_spec(str(p))[1])
    with pytest.raises(SpecError, match="pass that tables.yaml"):
        spec_from_kv(kv)
    assert spec_from_kv(kv, str(p))[1].sha256 == _sha(p)
    other = _write(tmp_path, _custom(spec_version="0.2"), "other.yaml")
    with pytest.raises(SpecError, match="were written by custom table spec"):
        spec_from_kv(kv, str(other))


# --- version --json --------------------------------------------------------------------------------------------


def test_version_info_lists_the_specs():
    info = compat.version_info()
    # the keys from before specs had names keep their meaning: the clinvar-gkm spec
    assert info["table_spec_version"] == compat.TABLE_SPEC_VERSION == "1.0"
    assert info["table_spec_status"] == load_spec("1.0").status
    assert info["tables"] == sorted(t.name for t in load_spec("1.0").tables)
    assert info["default_table_spec"] == "baseline"
    specs = {s["name"]: s for s in info["table_specs"]}
    assert set(specs) == {"baseline", "clinvar-gkm"}
    assert specs["baseline"]["default"] and not specs["clinvar-gkm"]["default"]
    assert specs["baseline"]["version"] == "1.0" and len(specs["baseline"]["tables"]) == 20
    assert len(specs["clinvar-gkm"]["tables"]) == 22
