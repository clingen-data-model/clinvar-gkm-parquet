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
