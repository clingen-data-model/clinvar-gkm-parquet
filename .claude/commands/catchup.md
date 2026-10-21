---
description: Brief me on what changed in this repo and in ClinVar-GKM since I last synced, focused on what I'm about to touch.
---

You are catching a contributor up on changes since they last synced. Produce a tight briefing, not a changelog
dump. Derive the picture from git and GitHub, never from a hand-maintained doc.

1. **This repo.** `git fetch --tags origin`. Unless the user named a range, use the last two weeks:
   `git log --oneline --since="2 weeks ago" origin/main`. List merged PRs in range with
   `gh pr list --state merged --base main --limit 50 --json number,title,mergedAt,url,files` and read the bodies
   of those touching what the user is about to work on (ask if they didn't say). Read new or changed ADRs,
   plans, `.claude/rules/*.md`, `CLAUDE.md`, the table spec and the compatibility matrix.
2. **ClinVar-GKM** (`clingen-data-model/clinvar-gkm`, local clone `~/Development/gks/clinvar-gks`). The tool
   depends on it, so check both `origin/main` (dev) and `origin/1.0` (release line) for changes in range to:
   - the bundle `schema_version` (`SCHEMA_VERSION` in `src/scripts/assemble-gkm-dicts.py`) and the bundle schema
     `schema/clinvar-gkm/clinvar-gkm-bundle.schema.json`: a new version the tool must accept or refuse;
   - the assembler's output format (`assemble-gkm-dicts.py`, `gkm_json_cleanup.py`): the line-oriented reader
     depends on it;
   - the BigQuery Parquet export (`src/scripts/parquet-schemas/*.sql`, `export-gkm-dicts.sh`,
     `release-gkm-parquet.sh`): the oracle's reference;
   - the delta manifest and index (`build-delta-manifest.py`, `generate-r2-index.sh`) and the release-model docs;
   - ADRs in `docs/decisions/` (especially 0004, 0007, 0008) and `docs/parquet/tables.md`.
   Use `git -C ~/Development/gks/clinvar-gks log --oneline --since=... origin/1.0 -- <paths>`.
3. **Published data.** Fetch `https://data.clinvar-gkm.org/index.json` and the current line's index
   (`/v1/1.1/index.json`, provisional path; send a User-Agent) and note any new line, `schema_version`, release,
   monthly `source_release` or Parquet manifest.
4. Summarize in a few bullets: what landed and why, what it means for the files the user named, any interface
   or convention change to adopt (especially a new bundle `schema_version` or a format change), and open
   follow-ups. Link PRs and ADRs by number.
