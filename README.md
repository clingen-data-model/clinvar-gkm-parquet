# clinvar-gkm-parquet

Builds queryable **Parquet** tables from **[ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm)**
JSON bundles.

The JSON bundles are the public record. Parquet is a derived convenience: a quick way to explore and query
the bundle content with current database tools such as DuckDB, Polars, pandas and Spark. This tool rebuilds
those Parquet tables from any bundle, including archived releases, so the Parquet for a release can always be
regenerated from the bundle it came from.

> **Status:** not yet released. The first code (v0, design plan §12 step 1) is merged and tested on a real
> excerpt of the published 2026-10-18 bundle. Table spec 1.0 is a draft: its names and types match ClinVar-GKM's
> published 1.1.0 Parquet export, and it is final once the oracle has run against that export
> ([design plan](docs/plans/2026-10-20-design.md) §3.2, §8.2). Until the tool ships, ClinVar-GKM builds its published
> Parquet set with its own BigQuery export.
>
> ClinVar-GKM publishes **one** Parquet dataset, at `https://data.clinvar-gkm.org/v1/1.1/parquet/`,
> overwritten every release, with a manifest naming the build date, source release and tool version. Past
> releases have no published Parquet; this tool rebuilds them from the monthly bundles (or a monthly plus
> deltas, for a past week). See [ClinVar-GKM ADR 0009](https://github.com/clingen-data-model/clinvar-gkm/blob/docs/adr-0008-bundle-record/docs/decisions/0009-r2-layout-mirrors-ncbi-clinvar.md)
> (proposed, PR #168). User documentation will live on the
> [ClinVar-GKM docs site](https://dataexchange.clinicalgenome.org/clinvar-gkm/), in a planned
> Ecosystem → Add-ons section.

## Versioning

This tool is versioned **independently** of the ClinVar-GKM bundle schema, because the two release on
different cadences ([ADR 0002](docs/decisions/0002-independent-tool-versioning.md)). Every release states:

- the bundle `schema_version`s it accepts as input (the tool reads each bundle's self-declared
  `schema_version` and refuses versions it does not support);
- the built-in Parquet **table specs** it writes, each with its own version, and which one is the default
  ([ADR 0003](docs/decisions/0003-pluggable-table-specs.md)). A spec defines the tables and columns.

| Tool version | Bundle input (`schema_version`) | Built-in table specs |
| --- | --- | --- |
| *(none released yet)* | | |
| *(unreleased `main`)* | 1.1.0 | `sections` 1.0 (default; draft), `baseline` 1.0 (draft), `clinvar-gkm` 1.0 (draft) |

- `sections`: one table per bundle section (20), keyed by `id`, with every record property its own typed column
  (named as the property; pointers as bare ids; extensions in `value_<type>` slots by the JSON type of their
  value) and no `data` column. A property, value type or record shape the spec doesn't cover stops the build.
- `baseline`: one table per bundle section (20), each `id`, `refs` (every `#/<section>/<id>` pointer in the
  record) and `data` (the record, byte for byte).
- `clinvar-gkm`: the 22 tables of ClinVar-GKM's published Parquet set, with typed columns matching its BigQuery
  export; the spec the oracle compares and the one ClinVar-GKM's release build names.

`version --json` gives the same (`table_specs`, `default_table_spec`). See the
[design plan](docs/plans/2026-10-20-design.md#92-compatibility-matrix-planned) for the planned releases.

Each Parquet file records, in its key-value metadata, the tool version, the spec's name and version (and, for a
custom spec, the sha256 of its file), the source bundles' sha256s and, for a selective build, the selection
([design plan](docs/plans/2026-10-20-design.md#72-key-value-metadata-every-file) §7.2).

## Usage (unreleased)

```sh
# The sections spec (the default): one table per bundle section, a typed column per record property
clinvar-gkm-parquet build --full clinvar-gkm_2026-1018.json.gz --out out/

# The baseline spec: one table per bundle section, each id, refs and the record verbatim (data)
clinvar-gkm-parquet build --spec baseline --full clinvar-gkm_2026-1018.json.gz --out out/

# ClinVar-GKM's published layout (the BigQuery-parity build), from a monthly plus the weeks after it
clinvar-gkm-parquet build --spec clinvar-gkm-1.0 --full clinvar-gkm_2026-10.json.gz \
    --delta clinvar-gkm-delta_2026-1025.json.gz --out out/

# Your own spec file: typed tables of any sections, no SQL needed
clinvar-gkm-parquet build --spec my-tables.yaml --full clinvar-gkm_2026-1018.json.gz --out out/

# A selection: one submitter and every record that points to it (its SCVs)
clinvar-gkm-parquet build --full clinvar-gkm_2026-1018.json.gz --out omim/ \
    --driver submitter --where "name = 'OMIM'" --follow in --max-depth 1

# The same selection's mini-bundle only (byte-identical to the one the build used)
clinvar-gkm-parquet select --full clinvar-gkm_2026-1018.json.gz --out omim.json.gz \
    --driver submitter --where "name = 'OMIM'" --follow in --max-depth 1

# --where reads the driver table as the chosen spec builds it: with --spec baseline, the record is `data`
clinvar-gkm-parquet build --spec baseline --full clinvar-gkm_2026-1018.json.gz --out omim/ \
    --driver submitter --where "data->>'\$.name' = 'OMIM'" --follow in --max-depth 1

# Checks: the directory on its own; a full build against its bundle; a selection against its mini-bundle
clinvar-gkm-parquet verify out/
clinvar-gkm-parquet verify out/ --bundle-recompute --full clinvar-gkm_2026-1018.json.gz
clinvar-gkm-parquet verify omim/ --bundle-recompute --full omim.json.gz
```

Selection options (`build` and `select`): `--driver <table>` (a table of the chosen spec), `--where` (a DuckDB
boolean expression over that table as the spec builds it), `--ids k1,k2` and `--ids-file` (added to the
`--where` keys; with neither, every row of the driver table), `--follow out|in|both` (default `out`),
`--max-depth N` and `--sections a,b,c`. See [ADR 0004](docs/decisions/0004-driver-based-selection.md) and the
[design plan](docs/plans/2026-10-20-design.md) §4.5 and §10. `verify --spec <file>` is required for a custom
spec; `verify --against` (the oracle) takes a full `clinvar-gkm` build only.

A spec file uses the format of the built-in ones (`spec/sections/1.0/tables.yaml`, `spec/1.0/tables.yaml`;
reference in `src/clinvar_gkm_parquet/spec.py`). Its tables need no SQL: each column names a derivation kind
(`scalar-path`, `bare-id`, `pointer-section`, `typed-copy`, `extension-slot`, `range-exact`, `range-min`,
`range-max`, `refs`, plus the `key` and an optional `record` column) and a path such as `$.name` or
`$.contributions[*].contributor`; extension slots go by name or, with `json-type`, by the JSON type of the value. Example:
[`tests/fixtures/custom-spec/tables.yaml`](tests/fixtures/custom-spec/tables.yaml). Only `computed` columns and
row filters need a SQL file the tool ships.

## Design records

- [Design plan](docs/plans/2026-10-20-design.md): scope, table spec, inputs and delta replay, engine, CLI,
  testing (including the oracle against ClinVar-GKM's current export), rollout, and open decisions.
- [Architecture decision records](docs/decisions/README.md):
  [0001 — Parquet is derived from the bundles](docs/decisions/0001-parquet-derived-from-bundles.md),
  [0002 — Independent tool versioning](docs/decisions/0002-independent-tool-versioning.md),
  [0003 — Pluggable table specs](docs/decisions/0003-pluggable-table-specs.md),
  [0004 — Driver-based selection](docs/decisions/0004-driver-based-selection.md).

## Contributing

Work on a branch and open a pull request against `main`. Project conventions are in [`CLAUDE.md`](CLAUDE.md)
and `.claude/rules/`.

## License

[CC0 1.0 Universal](LICENSE): dedicated to the public domain.
