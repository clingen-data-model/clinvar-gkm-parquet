# clinvar-gkm-parquet

Builds queryable **Parquet** tables from **[ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm)**
JSON bundles.

The JSON bundles are the public record. Parquet is a derived convenience: a quick way to explore and query
the bundle content with current database tools such as DuckDB, Polars, pandas and Spark. This tool rebuilds
those Parquet tables from any bundle, including archived releases, so the Parquet for a release can always be
regenerated from the bundle it came from.

> **Status:** not yet released; there is no code yet. The design is under review:
> [design plan](docs/plans/2026-10-20-design.md). Until the tool ships, ClinVar-GKM builds its published
> Parquet set with its own BigQuery export. User documentation will live on the
> [ClinVar-GKM docs site](https://dataexchange.clinicalgenome.org/clinvar-gkm/), in a planned
> Ecosystem → Add-ons section.

## Versioning

This tool is versioned **independently** of the ClinVar-GKM bundle schema, because the two release on
different cadences ([ADR 0002](docs/decisions/0002-independent-tool-versioning.md)). Every release states:

- the bundle `schema_version`s it accepts as input (the tool reads each bundle's self-declared
  `schema_version` and refuses versions it does not support);
- the Parquet **table-spec** version it writes, which defines the tables and columns.

| Tool version | Bundle input (`schema_version`) | Parquet table spec |
| --- | --- | --- |
| *(none released yet)* | | |

The first planned release reads bundle schema 1.1.0 and writes table spec 1.0; see the
[design plan](docs/plans/2026-10-20-design.md#92-compatibility-matrix-planned).

Each Parquet file the tool writes will record the tool version, the table-spec version and the source bundle's
sha256 in its metadata.

## Design records

- [Design plan](docs/plans/2026-10-20-design.md): scope, table spec, inputs and delta replay, engine, CLI,
  testing (including the oracle against ClinVar-GKM's current export), rollout, and open decisions.
- [Architecture decision records](docs/decisions/README.md):
  [0001 — Parquet is derived from the bundles](docs/decisions/0001-parquet-derived-from-bundles.md),
  [0002 — Independent tool versioning](docs/decisions/0002-independent-tool-versioning.md).

## Contributing

Work on a branch and open a pull request against `main`. Project conventions are in [`CLAUDE.md`](CLAUDE.md)
and `.claude/rules/`.

## License

[CC0 1.0 Universal](LICENSE): dedicated to the public domain.
