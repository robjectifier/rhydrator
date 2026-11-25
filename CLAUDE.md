@AGENTS.md

## Claude Code specifics

- Prefer plan mode for changes to `src/rhydrator/model.py`. RHydrator owns the
  database model, and model changes get reviewed before they land.
- Keep machine-specific notes (local paths, virtual environments, sibling
  checkouts) in `CLAUDE.local.md`, which is gitignored.
- In a guided PR (`AGENTS.md`, _Guided PRs_), leave the desktop app's Auto-fix
  off: it would fix the person's code for them.
