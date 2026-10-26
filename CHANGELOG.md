# Changelog

All notable changes to this tool. Each release states the bundle `schema_version`s it accepts and the table-spec
version it writes (ADR 0002). Tags are `vX.Y.Z`.

## Unreleased

- First tool code (design plan §12, step 1): package scaffold, CLI (`build`, `verify`, `version --json`),
  table spec 1.0 (draft), streaming bundle reader, delta replay, DuckDB table build, `verify`.
- Reads bundle `schema_version` 1.1.0; writes table spec 1.0 (draft until captured from the 1.1.0 export).
- Streaming build engine (design plan §6.2): the full is read once, section by section, and each section's
  tables are built in a worker thread while the next section is read. There is no state database: the deltas
  are replayed in the stream from an in-memory change map, and only the current sections are staged.
  `verify --bundle-recompute` streams the same way. The Parquet files are byte-identical to the first engine's.
  New refusals: a full whose sections are out of the assembler's order. A full with no published sha256 is
  hashed in a first pass (its tables' metadata names it).
- Table specs on the CLI (ADR 0003): `build --spec <name|path>` and `verify --spec`. **Behaviour change:** with
  no `--spec`, `build` now writes the built-in `baseline` spec (20 tables: `id`, `refs`, `data`); the 22-table
  layout of the published set is `--spec clinvar-gkm-1.0` (also `clinvar-gkm`, `clinvar-gkm@1.0`). Every file
  gains the key-value entry `clinvar_gkm_parquet.table_spec` (plus `table_spec_builtin` and `table_spec_sha256`
  for a custom spec), and the manifest a `table_spec` object. That entry is the only byte change to a
  `clinvar-gkm` build: on the real excerpt, with and without deltas, every table's rows equal the previous
  build's, and the old key set gives byte-identical files. `verify` reads the spec from the files; a custom spec
  needs `--spec <file>` with the recorded sha256. A spec may now read a subset of the bundle sections; the other
  sections are still read, checked and counted. `version` and `version --json` list the built-in specs
  (`table_specs`, `default_table_spec`).
- Selective builds (ADR 0004): `build --driver <table>` with `--where`, `--ids`, `--ids-file`, `--follow`,
  `--max-depth` and `--sections`, and a new `select` command that writes only the mini-bundle. `--where` is a
  DuckDB expression evaluated on the driver table as the chosen spec builds it, on the replayed state when
  deltas are given. Every file and the manifest of a selective build carry `clinvar_gkm_parquet.selection`;
  the source keys still name the published bundles. `verify --bundle-recompute` checks a selection against its
  mini-bundle (`--full`, whose sha256 must equal `selected_sha256`), and `verify --against` refuses a selection
  or a set not built with `clinvar-gkm`. A build with no driver never runs the selection engine.
