# clinvar-gkm-parquet

Builds queryable **Parquet** tables from **[ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm)**
JSON bundles.

The JSON bundles are the public record. Parquet is a derived convenience: a quick way to explore and query
the bundle content with current database tools such as DuckDB, Polars, pandas and Spark. This tool rebuilds
those Parquet tables from any bundle, including archived releases, so the Parquet for a release can always be
regenerated from the bundle it came from.

> **Status:** not yet released. The first code (v0, design plan §12 step 1) is merged and tested on a real
> excerpt of the published 2026-10-18 bundle. The `clinvar-gkm` table spec 1.0 is a draft: its names and types match
> ClinVar-GKM's published 1.1.0 Parquet export, and it is final once the oracle has run against that export
> ([design plan](docs/plans/2026-10-20-design.md) §3.2, §8.2). The default `sections` spec is compiled from a bundle schema
> and its column set, for any producer ([ADR 0006](docs/decisions/0006-sections-spec-compiled-from-schemas.md), proposed);
> that path is planned to move to the GKM Toolkit. Until the tool ships, ClinVar-GKM builds its published
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

- the built-in Parquet **table specs** it writes, each with its own version, and which one is the default
  ([ADR 0003](docs/decisions/0003-pluggable-table-specs.md)). A spec defines the tables and columns;
- the bundles they read. `sections` compiles a table spec for **any bundle plus its schema and column-set file**
  ([ADR 0006](docs/decisions/0006-sections-spec-compiled-from-schemas.md),
  [format](docs/column-set-format.md)). With neither given it reads the tool's default, ClinVar-GKM bundles, by the
  bundle's own `schema_version`: the snapshotted versions work offline, any other is fetched once and cached, and a version
  whose pairing can't load or compile is refused with the reason. `baseline`, `clinvar-gkm` and a custom spec read the
  versions they list and refuse any other.

| Tool version | `sections` reads | `baseline`, `clinvar-gkm` read | Built-in table specs |
| --- | --- | --- | --- |
| *(none released yet)* | | | |
| *(unreleased `main`)* | any bundle with its pairing; ClinVar-GKM by default (snapshot: 1.1.0) | 1.1.0 | `sections` 1.0 (default; draft), `baseline` 1.0 (draft), `clinvar-gkm` 1.0 (draft) |

- `sections`: one table per bundle collection (20 in ClinVar-GKM 1.1.0), keyed by `id`, compiled from a bundle schema and
  its column set, with every record property its own typed column
  (named as the property; pointers as bare ids; extensions in `value_<type>` slots by the JSON type of their
  value) and no `data` column. A property, value type or record shape the spec doesn't cover stops the build.
  It is lossless ([ADR 0005](docs/decisions/0005-lossless-sections-build.md)): each table also has `ordinal`
  (the record's position in its section) and, for a pointer with several target sections, `<column>_section`, and root
  values that aren't collections go in a `root_values` table. The build checks every row. `rebuild` writes back a bundle in
  the line layout byte for byte; any other layout JSON-equal, in the canonical line layout. The manifest says which.
- `baseline`: one table per bundle section (20), each `id`, `refs` (every `#/<section>/<id>` pointer in the
  record) and `data` (the record, byte for byte).
- `clinvar-gkm`: the 22 tables of ClinVar-GKM's published Parquet set, with typed columns matching its BigQuery
  export; the spec the oracle compares and the one ClinVar-GKM's release build names.

`version --json` gives the same (`table_specs`, `default_table_spec`, `schema_version_rule`,
`snapshotted_schema_versions`). See the
[design plan](docs/plans/2026-10-20-design.md#92-compatibility-matrix-planned) for the planned releases.

Each Parquet file records, in its key-value metadata, the tool version, the spec's name and version (for a custom spec, also
the sha256 of its file; for `sections`, the sha256 of its schema set, its column set and what its compiled spec makes of
the tables), the source bundles' sha256s, for a lossless build the sha256 of its section's text and, for a selective build,
the selection ([design plan](docs/plans/2026-10-20-design.md#72-key-value-metadata-every-file) §7.2). A `sections` set also
holds its compiled spec (`table-spec.json`), so `verify` and `rebuild` need neither the schema nor the column set.

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

# Any producer's bundle, with its schema and column set (docs/column-set-format.md)
clinvar-gkm-parquet build --bundle-schema my.schema.json --column-set my.columns.yaml --full my-bundle.json --out out/

# The columns a pairing or a default version gives, with no build
clinvar-gkm-parquet compile-spec --bundle-schema my.schema.json --column-set my.columns.yaml
clinvar-gkm-parquet compile-spec 1.1.0 > sections-1.1.0.yaml

# Back from Parquet to the bundle: a full sections build rebuilds the bundle's text byte for byte
clinvar-gkm-parquet rebuild out/ -o clinvar-gkm_2026-1018.json.gz

# Checks: the directory on its own; a full build against its bundle; a selection against its mini-bundle;
# the round trip (the rebuilt text against the recorded hashes and, with --full, the bundle's own text)
clinvar-gkm-parquet verify out/
clinvar-gkm-parquet verify out/ --bundle-recompute --full clinvar-gkm_2026-1018.json.gz
clinvar-gkm-parquet verify omim/ --bundle-recompute --full omim.json.gz
clinvar-gkm-parquet verify out/ --roundtrip --full clinvar-gkm_2026-1018.json.gz
```

`rebuild` writes gzip when the output name ends in `.gz`; the gzip bytes differ from the published file (header
and compressor), the uncompressed text does not, and its sha256 is checked against the build's manifest
(`source_text_sha256`) and each file's `section_text_sha256`. It refuses anything it can't reproduce exactly:
another spec, a selection, a build with deltas or `--tables`, a missing table, manifest or ordinal, a pointer outside its
targets ([ADR 0005](docs/decisions/0005-lossless-sections-build.md)).

Selection options (`build` and `select`): `--driver <table>` (a table of the chosen spec), `--where` (a DuckDB
boolean expression over that table as the spec builds it), `--ids k1,k2` and `--ids-file` (added to the
`--where` keys; with neither, every row of the driver table), `--follow out|in|both` (default `out`),
`--max-depth N` and `--sections a,b,c`. See [ADR 0004](docs/decisions/0004-driver-based-selection.md) and the
[design plan](docs/plans/2026-10-20-design.md) §4.5 and §10. `verify --spec <file>` is required for a custom
spec; `verify --against` (the oracle) takes a full `clinvar-gkm` build only.

A spec file uses the format of the built-in ones (`spec/1.0/tables.yaml`, or `compile-spec` for `sections`;
reference in `src/clinvar_gkm_parquet/spec.py`). Its tables need no SQL: each column names a derivation kind
(`scalar-path`, `bare-id`, `pointer-section`, `typed-copy`, `extension-slot`, `range-exact`, `range-min`,
`range-max`, `refs`, `ordinal`, plus the `key` and an optional `record` column) and a path such as `$.name` or
`$.contributions[*].contributor`; extension slots go by name or, with `json-type`, by the JSON type of the value.
A spec that should rebuild the bundle states `lossless: true`, an `ordinal` column and a `json` block per table,
and the `targets` of every pointer (as `compile-spec`'s output does). Example:
[`tests/fixtures/custom-spec/tables.yaml`](tests/fixtures/custom-spec/tables.yaml). Only `computed` columns and
row filters need a SQL file the tool ships.

## Design records

- [Design plan](docs/plans/2026-10-20-design.md): scope, table spec, inputs and delta replay, engine, CLI,
  testing (including the oracle against ClinVar-GKM's current export), rollout, and open decisions.
- [Architecture decision records](docs/decisions/README.md):
  [0001 — Parquet is derived from the bundles](docs/decisions/0001-parquet-derived-from-bundles.md),
  [0002 — Independent tool versioning](docs/decisions/0002-independent-tool-versioning.md),
  [0003 — Pluggable table specs](docs/decisions/0003-pluggable-table-specs.md),
  [0004 — Driver-based selection](docs/decisions/0004-driver-based-selection.md),
  [0005 — A full sections build is lossless](docs/decisions/0005-lossless-sections-build.md).

## Contributing

Work on a branch and open a pull request against `main`. Project conventions are in [`CLAUDE.md`](CLAUDE.md)
and `.claude/rules/`.

## License

[CC0 1.0 Universal](LICENSE): dedicated to the public domain.
