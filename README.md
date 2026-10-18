# clinvar-gkm-parquet

Builds queryable **Parquet** tables from **[ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm)**
JSON bundles.

The JSON bundles are the public record. Parquet is a derived convenience: a quick way to explore and query
the bundle content with current database tools such as DuckDB, Polars, pandas and Spark. This tool rebuilds
those Parquet tables from any bundle, including archived releases, so the Parquet for a release can always be
regenerated from the bundle it came from.

> **Status:** not yet released. Planning is under way; see the ClinVar-GKM docs site
> (https://dataexchange.clinicalgenome.org/clinvar-gkm), Ecosystem → Add-ons.

## Versioning

This tool is versioned **independently** of the ClinVar-GKM bundle schema, because the two release on
different cadences. Every release states:

- the bundle `schema_version`s it accepts as input (the tool reads each bundle's self-declared
  `schema_version` and refuses versions it does not support);
- the Parquet **table-spec** version it writes, which defines the tables and columns.

| Tool version | Bundle input (`schema_version`) | Parquet table spec |
| --- | --- | --- |
| *(none yet)* | | |

Each Parquet file the tool writes records the tool version, the table-spec version and the source bundle's
sha256 in its metadata.

## License

[CC0 1.0 Universal](LICENSE): dedicated to the public domain.
