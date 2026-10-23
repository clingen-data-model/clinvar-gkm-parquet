# Changelog

All notable changes to this tool. Each release states the bundle `schema_version`s it accepts and the table-spec
version it writes (ADR 0002). Tags are `vX.Y.Z`.

## Unreleased

- First tool code (design plan §12, step 1): package scaffold, CLI (`build`, `verify`, `version --json`),
  table spec 1.0 (draft), streaming bundle reader, delta replay, DuckDB table build, `verify`.
- Reads bundle `schema_version` 1.1.0; writes table spec 1.0 (draft until captured from the 1.1.0 export).
