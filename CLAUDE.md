@AGENTS.md

## Claude Code specifics

- Prefer plan mode for changes to `src/rhydrator/model.py`: objectservice
  depends on the database model, so changing a table is not a local decision.
- Keep machine-specific notes (local paths, virtual environments, sibling
  checkouts) in `CLAUDE.local.md`, which is gitignored.
