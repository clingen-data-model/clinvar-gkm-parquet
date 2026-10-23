import json

from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.compat import ACCEPTED_SCHEMA_VERSIONS, TABLE_SPEC_VERSION


def test_version_json(capsys):
    assert main(["version", "--json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["table_spec_version"] == TABLE_SPEC_VERSION
    assert info["accepted_schema_versions"] == list(ACCEPTED_SCHEMA_VERSIONS)
    assert info["duckdb_version"] == "1.5.4"
    assert info["tables"]
