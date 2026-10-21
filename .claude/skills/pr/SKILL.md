---
name: pr
description: Open a pull request for clinvar-gkm-parquet. Use when the user asks to commit and open a PR, or to push a branch for review.
---

# Create a PR

1. `git status` and `git diff --stat` to see what changed. Commit only the files the user asked for.
2. If you are on `main`, create a branch first. Name it `<type>/<short-topic>`, where type is one of `feat`,
   `fix`, `docs`, `chore`, `ci`, `spec` (for a table-spec change).
3. Check the lockstep rules before committing:
   - a change to CLI behaviour, the table spec, accepted `schema_version`s or the compatibility matrix also
     updates `README.md`, the changelog and the affected plan or ADR (see `.claude/rules/docs.md`);
   - a table-spec change follows `.claude/rules/table-spec.md` (new spec directory, bump level, conformance
     test, oracle or recompute result).
4. Run what exists: `uv run ruff check`, `uv run pytest` (once the package exists). Don't claim a check passed
   unless you ran it.
5. Commit with a short conventional message (`docs: …`, `feat(cli): …`). **No** "Co-Authored-By: Claude" and
   **no** "Generated with Claude Code", in the commit or in the PR body.
6. Push with `git push -u origin <branch>`.
7. Open the PR against `main`, as a draft unless the user says otherwise:
   `gh pr create --draft --base main --title "<title>" --body-file <file>`.
   The body says what changed, why, how it was verified (commands and results), what was not verified, and
   which ClinVar-GKM docs page needs a paired change, if any.
8. Don't merge. Report the PR URL to the user.
