# clinvar-gkm-parquet

Builds queryable **Parquet** tables from **GKM JSON bundles**, rebuilds the bundle from them, and validates bundles
against their JSON Schema. It works with any bundle built on the GA4GH GKM classes (VRS, Cat-VRS, VA-Spec), given that
bundle's JSON Schema. Its first producer is [ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm). The tests also
run on real [CIViC](https://github.com/ga4gh/gkm-starter-kit) bundles, which differ from ClinVar-GKM's in layout, version
and root values.

The JSON bundles are the public record. Parquet is a derived convenience: a quick way to explore and query the bundle
content with current database tools such as DuckDB, Polars, pandas and Spark. Every Parquet value can be recomputed from
the bundle it came from ([ADR 0001](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0001-parquet-derived-from-bundles.md)).

> **Status:** 1.0.1 is the latest release. Two accepted or proposed decisions reshape the tool for 2.0.0 (planned):
>
> - [ADR 0008](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0008-end-state-reversible-default-custom-projections-filters.md)
>   (accepted): the default Parquet set is generated from the bundle schema alone and rebuilds the bundle record for
>   record. Custom column sets are labelled projections, and filters work in both directions. The `clinvar-gkm` and
>   `baseline` specs and hand-written `tables.yaml` input are retired.
> - [ADR 0009](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0009-generic-gkm-bundle-tool-renamed.md)
>   (proposed): the tool names no producer and is renamed **`gkm-bundle-parquet`**. Every command takes the bundle's
>   schema (`--bundle-schema`), and none defaults to ClinVar-GKM. ClinVar-GKM's release model (its line index, monthly,
>   weekly and delta files, and replay) moves to ClinVar-GKM.
>
> A 1.x release will deprecate what 2.0.0 removes. Until then, the commands below work as documented, including the
> ClinVar-GKM defaults. ClinVar-GKM plans to publish the default set this tool builds as its Parquet dataset; today it
> publishes its own BigQuery export.

## Versioning

This tool is versioned **independently** of the ClinVar-GKM bundle schema, because the two release on
different cadences ([ADR 0002](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0002-independent-tool-versioning.md)). Every release states:

- the built-in Parquet **table specs** it writes, each with its own version, and which one is the default
  ([ADR 0003](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0003-pluggable-table-specs.md)). A spec defines the tables and columns;
- the bundles they read. `sections` compiles a table spec for **any bundle plus its schema and column-set file**
  ([ADR 0006](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0006-sections-spec-compiled-from-schemas.md),
  [format](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/column-set-format.md)). With neither given it reads the tool's default, ClinVar-GKM bundles, by the
  bundle's own `schema_version`: the snapshotted versions work offline, any other is fetched once and cached, and a version
  whose pairing can't load or compile is refused with the reason. `baseline`, `clinvar-gkm` and a custom spec read the
  versions they list and refuse any other.

| Tool version | `sections` reads | `baseline`, `clinvar-gkm` read | Built-in table specs |
| --- | --- | --- | --- |
| 1.0.0 | any bundle with its pairing; ClinVar-GKM by default (snapshot: 1.1.0) | 1.1.0 | `sections` 1.0 (default; draft), `baseline` 1.0 (draft), `clinvar-gkm` 1.0 (draft) |
| 1.0.1 | any bundle with its pairing; ClinVar-GKM by default (snapshot: 1.1.0) | 1.1.0 | `sections` 1.0 (default; draft), `baseline` 1.0 (draft), `clinvar-gkm` 1.0 (draft) |

- `sections`: one table per bundle collection (20 in ClinVar-GKM 1.1.0), keyed by `id`, compiled from a bundle schema and
  its column set, with every record property its own typed column
  (named as the property; pointers as bare ids; extensions in `value_<type>` slots by the JSON type of their
  value) and no `data` column. A property, value type or record shape the spec doesn't cover stops the build.
  It is lossless ([ADR 0005](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0005-lossless-sections-build.md)): each table also has `ordinal`
  (the record's position in its section) and, for a pointer with several target sections, `<column>_section`, and root
  values that aren't collections go in a `root_values` table. The build checks every row. `rebuild` writes back a bundle in
  the line layout byte for byte; any other layout JSON-equal, in the canonical line layout. The manifest says which.
- `baseline`: one table per bundle section (20), each `id`, `refs` (every `#/<section>/<id>` pointer in the
  record) and `data` (the record, byte for byte).
- `clinvar-gkm`: the 22 tables of ClinVar-GKM's published Parquet set, with typed columns matching its BigQuery
  export; the spec the oracle compares and the one ClinVar-GKM's release build names.

`version --json` gives the same (`table_specs`, `default_table_spec`, `schema_version_rule`,
`snapshotted_schema_versions`). See the
[design plan](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/plans/2026-10-20-design.md#92-compatibility-matrix-planned) for the planned releases.

Each Parquet file records, in its key-value metadata, the tool version, the spec's name and version (for a custom spec, also
the sha256 of its file; for `sections`, the sha256 of its schema set, its column set and what its compiled spec makes of
the tables), the source bundles' sha256s, for a lossless build the sha256 of its section's text and, for a selective build,
the selection ([design plan](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/plans/2026-10-20-design.md#72-key-value-metadata-every-file) §7.2). A `sections` set also
holds its compiled spec (`table-spec.json`), so `verify` and `rebuild` need neither the schema nor the column set.

## Install

```sh
uvx clinvar-gkm-parquet@1.0.1 version --json   # run a pinned release with no install
pipx install clinvar-gkm-parquet==1.0.1        # or install it
```

Python 3.10 or later. DuckDB is pinned exactly, because the output bytes depend on it, so an isolated environment
(`uvx` or `pipx`) avoids a clash with your own DuckDB.

`validate --engine python`, the reference validator, needs the `reference` extra (the GA4GH GKM Toolkit, `ga4gh.gkm`
0.4.0, Python 3.11 or later), whose GA4GH dependencies are pre-releases:
`uv tool install --prerelease=allow 'clinvar-gkm-parquet[reference]'` or `pip install --pre 'clinvar-gkm-parquet[reference]'`.
The default engine needs nothing extra.

## Usage

The examples use ClinVar-GKM bundles, the tool's default today. Any GKM bundle works with its schema and column set
(`--bundle-schema` and `--column-set`; [format](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/column-set-format.md)). The
ClinVar-GKM-only parts go in 2.0.0 (planned; ADRs 0008 and 0009): the `clinvar-gkm` and `baseline` specs and custom
`tables.yaml` input, `--release`, `--index`, `--delta`, `--manifest` and `--full-source-release`, and building with no
`--bundle-schema`.

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

# Validate every record against the bundle's JSON Schema set (the default engine, jsonschema-rs; docs/validate.md)
clinvar-gkm-parquet validate clinvar-gkm_2026-1018.json.gz
clinvar-gkm-parquet validate clinvar-gkm-delta_2026-1031.json.gz --bundle-schema my-schema-set/ --failures bad.jsonl
# The same with the GKM Toolkit's validator, the reference the default is checked against (much slower)
clinvar-gkm-parquet validate clinvar-gkm-delta_2026-1031.json.gz --engine python

# Checks: the directory on its own; a full build against its bundle; a selection against its mini-bundle;
# the round trip (the rebuilt text against the recorded hashes and, with --full, the bundle's own text)
clinvar-gkm-parquet verify out/
clinvar-gkm-parquet verify out/ --bundle-recompute --full clinvar-gkm_2026-1018.json.gz
clinvar-gkm-parquet verify omim/ --bundle-recompute --full omim.json.gz
clinvar-gkm-parquet verify out/ --roundtrip --full clinvar-gkm_2026-1018.json.gz
```

`validate` checks each record alone, as a one-section bundle with the bundle's `schema_version`, so key patterns are
checked too, as ClinVar-GKM's validator does. It exits 1 if any record fails, naming its section, key and instance paths,
and 2 on anything that isn't a verdict on a record (an unreadable bundle, an unknown section, a missing version, a schema
set that doesn't load). Its two engines give the same verdicts on every record of the 2026-10-31 delta and a sample of the
2026-10-31 full, with the published 1.1.0 schemas and ClinVar-GKM's narrowed ones; [docs/validate.md](docs/validate.md) has
the evidence, the known differences and the timings.

`rebuild` writes gzip when the output name ends in `.gz`; the gzip bytes differ from the published file (header
and compressor), the uncompressed text does not, and its sha256 is checked against the build's manifest
(`source_text_sha256`) and each file's `section_text_sha256`. It refuses anything it can't reproduce exactly:
another spec, a selection, a build with deltas or `--tables`, a missing table, manifest or ordinal, a pointer outside its
targets ([ADR 0005](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0005-lossless-sections-build.md)).

Selection options (`build` and `select`): `--driver <table>` (a table of the chosen spec), `--where` (a DuckDB
boolean expression over that table as the spec builds it), `--ids k1,k2` and `--ids-file` (added to the
`--where` keys; with neither, every row of the driver table), `--follow out|in|both` (default `out`),
`--max-depth N` and `--sections a,b,c`. See [ADR 0004](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0004-driver-based-selection.md) and the
[design plan](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/plans/2026-10-20-design.md) §4.5 and §10. `verify --spec <file>` is required for a custom
spec; `verify --against` (the oracle) takes a full `clinvar-gkm` build only.

A spec file uses the format of the built-in ones (`spec/1.0/tables.yaml`, or `compile-spec` for `sections`;
reference in `src/clinvar_gkm_parquet/spec.py`). Its tables need no SQL: each column names a derivation kind
(`scalar-path`, `bare-id`, `pointer-section`, `typed-copy`, `extension-slot`, `range-exact`, `range-min`,
`range-max`, `refs`, `ordinal`, plus the `key` and an optional `record` column) and a path such as `$.name` or
`$.contributions[*].contributor`; extension slots go by name or, with `json-type`, by the JSON type of the value.
A spec that should rebuild the bundle states `lossless: true`, an `ordinal` column and a `json` block per table,
and the `targets` of every pointer (as `compile-spec`'s output does). Example:
[`tests/fixtures/custom-spec/tables.yaml`](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/tests/fixtures/custom-spec/tables.yaml). Only `computed` columns and
row filters need a SQL file the tool ships.

## Design records

- [Design plan](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/plans/2026-10-20-design.md): scope, table spec, inputs and delta replay, engine, CLI,
  testing (including the oracle against ClinVar-GKM's current export), rollout, and open decisions. It records the
  ClinVar-GKM-specific design; ADRs 0008 and 0009 replace parts of it.
- [Architecture decision records](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/README.md):
  [0001 — Parquet is derived from the bundles](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0001-parquet-derived-from-bundles.md),
  [0002 — Independent tool versioning](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0002-independent-tool-versioning.md),
  [0003 — Pluggable table specs](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0003-pluggable-table-specs.md),
  [0004 — Driver-based selection](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0004-driver-based-selection.md),
  [0005 — A full sections build is lossless](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0005-lossless-sections-build.md),
  [0006 — Tables compiled from a bundle schema and its column set](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0006-sections-spec-compiled-from-schemas.md),
  [0008 — The end state](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0008-end-state-reversible-default-custom-projections-filters.md),
  [0009 — A generic GKM bundle tool, renamed gkm-bundle-parquet](https://github.com/clingen-data-model/clinvar-gkm-parquet/blob/main/docs/decisions/0009-generic-gkm-bundle-tool-renamed.md).

## Contributing

Work on a branch and open a pull request against `main`. Project conventions are in [`CLAUDE.md`](CLAUDE.md)
and `.claude/rules/`.

## License

[CC0 1.0 Universal](LICENSE): dedicated to the public domain.
