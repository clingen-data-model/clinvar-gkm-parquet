import pytest

from clinvar_gkm_parquet.compat import ACCEPTED_SCHEMA_VERSIONS, TABLE_SPEC_VERSION
from clinvar_gkm_parquet.errors import SpecError
from clinvar_gkm_parquet.spec import load_spec, parse_spec


def test_spec_loads_and_matches_compat():
    spec = load_spec(TABLE_SPEC_VERSION)
    assert spec.spec_version == TABLE_SPEC_VERSION
    assert tuple(spec.bundle_schema_versions) == ACCEPTED_SCHEMA_VERSIONS
    assert spec.tables, "the spec lists no tables"
    for t in spec.tables:
        assert t.column(t.sort_key)


def _minimal(**table_overrides):
    table = {
        "name": "t",
        "sections": ["s"],
        "row_filter": None,
        "sort_key": "id",
        "description": "",
        "columns": [
            {
                "name": "id",
                "type": "VARCHAR",
                "nulls": "never",
                "derivation": {"kind": "key", "path": None, "transform": None},
                "description": "",
            }
        ],
    }
    table.update(table_overrides)
    return {
        "spec_version": "9.9",
        "status": "draft",
        "bundle_schema_versions": ["1.1.0"],
        "sources": [],
        "writer": {},
        "tables": [table],
    }


def test_parse_minimal():
    assert parse_spec(_minimal()).table("t").describe() == [("id", "VARCHAR")]


@pytest.mark.parametrize(
    "overrides",
    [
        {"sort_key": "missing"},
        {"surprise": 1},
        {"extension_slots": {"nope": {"a": "value_string"}}},
    ],
)
def test_parse_rejects(overrides):
    with pytest.raises(SpecError):
        parse_spec(_minimal(**overrides))
