# AGENTS.md

## What this package is

RHydrator is a Python library for **dehydrating** ROOT RNTuple files into
metadata plus independently stored page bytes, and **rehydrating** metadata and
pages back into the original ROOT files, byte for byte. The metadata lives in a
relational database; the page bytes go to an object store.

It is one piece of the CMS Object Stores / Object Lakehouse project. CMS's
file-based data tiers (AOD -> MiniAOD -> NanoAOD) copy unchanged content forward
on every reprocessing. If pages are addressed individually, a new "version" of a
dataset is a diff rather than a full copy. RNTuple makes this feasible because
pages are already referenced by _locators_, and the locator is extensible beyond
an intra-file byte offset.

The work is planned in the milestone
[Phase 1 (local)](https://github.com/robjectifier/rhydrator/milestone/1): one
issue per PR, each with its scope, the test that proves it, its dependencies and
the decisions it rests on. Read the issue before starting on it.

## Design rules

These are decided. A PR implements them; changing one is a decision for the
maintainers, raised as a question, not made in code. Where older text (a
comment, a docstring, an old design note) disagrees with them or with an issue,
the rules and the issue win; ask if unsure.

- **Phase 1 is local:** page objects in a local folder, metadata in SQLite. S3
  and PostgreSQL come later, in objectservice; write SQL that works in both.
- **Byte-exact first.** A rehydrated file has exactly the original's bytes:
  identical size, adler32 and MD5.
- **rhydrator owns the database model** (`src/rhydrator/model.py`) and writes
  the SQL itself. objectservice, the service side, adapts to it, not the other
  way round.
- **Rehydration-only information** (the non-page bytes and where each page goes
  back) lives in the rehydration record and `rehydration_info`, never in the
  RNTuple tables. Those tables model meaning, and may adapt the spec rather than
  transcribe it.
- **No stored source positions.** Field, column, cluster and cluster-group IDs,
  representation indices and page indices in a file are list positions:
  translate them to database IDs during ingestion, and never let one become a
  foreign key or a stored ordinal. Kept on purpose: a column's stream index;
  each page's first element in its cluster (which orders a page group's pages);
  a cluster's first entry in the source, as provenance only.
- **Object IDs come from the database:** object rows are reserved as pending,
  the objects written under those IDs, then everything committed. Never derive
  an object ID from content or from positions in the file.
- **A page group is one column representation in one cluster.** Objects bundle
  page groups through a pluggable bundler, never across a cluster or an RNTuple.
- **Each page is stored once.** A page row records its object and its offset in
  it; pages that ROOT merged stay merged, as several page rows pointing at one
  object and offset.
- **A dataset's files share one schema.** Fields are identified by path; a field
  present in both the file and the dataset must match entirely, children
  included, or the ingest fails. A file may lack or add whole top-level fields;
  a new encoding adds a representation.
- **Files:** anything rootfilespec reads, tested on files from ROOT 6.36 (the
  CMS files) and 6.40.04 (the fixtures). Attribute sets are out of scope: their
  bytes stay non-page bytes. A file rhydrator can't handle fails with a clear
  error naming the file and the reason, and nothing of it is committed.

## The format reference

The reference for the on-disk format is ROOT's RNTuple Binary Format
Specification as tracked, verbatim, by
[root-io-spec](https://github.com/ariostas/root-io-spec):
[`spec/05-rntuple/BinaryFormatSpecification.md`](https://github.com/ariostas/root-io-spec/blob/d1618ad96c7e55277b291eb9e3af4ee8d0c359d9/spec/05-rntuple/BinaryFormatSpecification.md)
(v1.0.2.1, release 2026.09.24). Read it with
[`ERRATA.md`](https://github.com/ariostas/root-io-spec/blob/d1618ad96c7e55277b291eb9e3af4ee8d0c359d9/spec/05-rntuple/ERRATA.md)
and
[`NOTES.md`](https://github.com/ariostas/root-io-spec/blob/d1618ad96c7e55277b291eb9e3af4ee8d0c359d9/spec/05-rntuple/NOTES.md)
beside it: ROOT's document disagrees with ROOT's code in places, and those files
record where, with source citations and bytes. When the question is "what does
the format actually say", read these, not `rootfilespec`'s parser: the parser
implements the spec and can diverge from it.

Related repositories:

- [`rootfilespec`](https://github.com/nsmith-/rootfilespec) parses ROOT files
  and RNTuple envelopes; it is rhydrator's reader. Its problems are fixed there
  (see below), never worked around here.
- objectservice: the service side (S3 endpoint, ingestion). It adapts to
  rhydrator's database model; see _Design rules_.

## Python environment

- Package code is in `src/rhydrator/`; tests mirror it under `tests/`.
- `requires-python = ">=3.10"`. Target 3.10+ unless told otherwise.
- Install with `uv sync`. Run project code with `uv run`.
- root-io-spec's test fixtures are the submodule `reference/root-io-spec`:
  `git submodule update --init reference/root-io-spec` after cloning. Never
  `--recursive`: root-io-spec's own submodule is ROOT's source tree (1.5 GB).
- Never edit `.venv/`, `uv.lock` (not committed) or `src/rhydrator/_version.py`
  (generated by hatch-vcs).
- There is **no** `migrations/` directory. Do not assume one.
- **rhydrator tracks the latest PyPI release of rootfilespec**, now `0.1.0`.
  rootfilespec is what tracks ROOT's format.
  - Do **not** add a path or git dependency on a rootfilespec checkout to make
    something work. Unreleased rootfilespec changes are not rhydrator's concern
    until they are released.
  - **Keep every rootfilespec call in one module** (`reader.py`, #9), so a
    release that changes its API is a one-file port rather than a hunt through
    the package. The one exception, `layoutviz.py`, ends with its port (#18).
  - When reader behaviour looks wrong, check which rootfilespec is installed
    before debugging anything else:

    ```sh
    uv run python -c 'import rootfilespec; print(rootfilespec.__version__)'
    ```

## Validation

- Tests: `uv run pytest`
- Everything CI's Format job runs (ruff, ruff format, mypy, prettier and the
  rest, at the versions pinned in `.pre-commit-config.yaml`):
  `uvx pre-commit run --all-files --hook-stage manual`
- One hook: `uvx pre-commit run mypy --all-files` (config in `[tool.mypy]`;
  `strict` is off during early development)

Ruff and mypy are **not** declared in any dependency group, so `uv run ruff` /
`uv run mypy` fail, and a bare `uvx mypy` lacks the hook's dependencies
(rootfilespec, SQLAlchemy). Run them through pre-commit.

## Working agreements

- Add or update tests when behaviour changes.
- Prefer small committed RNTuple fixtures (root-io-spec's `data/rntuple/`,
  `scikit-hep-testdata`) over large files. Large files come only through the
  `RHYDRATOR_LARGE_FILES` environment variable.
- Tests must not depend on paths outside the repository, and must not silently
  pass when a data file is missing: skip explicitly instead.
- Do not add dependencies without asking.
- Problems that rootfilespec needs to fix are out of scope here. File them as
  issues on [`nsmith-/rootfilespec`](https://github.com/nsmith-/rootfilespec)
  and do not work around them in rhydrator. The two are developed in parallel:
  what rhydrator needs from rootfilespec (for example, to write files) is filed
  there too, as a request.

## How we work

**Issues.**

- One issue per PR. File a problem as an issue before fixing it, with the
  evidence (file, offset, spec section, the call that fails).
- Before starting on an existing issue, comment with the approach you'll take.
- Anything new found along the way becomes a new issue. Don't fix it silently in
  an unrelated PR.

**Branches and PRs.**

- One branch per PR, off `main`: `feat/<issue>-<slug>` for a new feature,
  `fix/<issue>-<slug>` for a bug fix.
- A PR built on another says **Depends on #N** and is rebased when that one
  changes or merges.
- After every merge, rebase the remaining open PRs onto the new `main`, re-run
  everything, and update the counts in their descriptions.

**Commits.** Small and focused. The message says what changed and why, citing
the spec where it applies.

**Tests.**

- Every behaviour change gets a test that fails on `main` and passes on the
  branch. Check both, and say which tests fail on `main`.
- Prefer real fixtures to synthetic bytes. When a fixture's `case.toml` pins
  bytes at an offset, assert exactly those.
- Name a test module after what it tests.

**Checks before every push.**

- Run the same tool versions as CI: read the pins from `.pre-commit-config.yaml`
  rather than using whatever is installed.
- Run lint, format check, type check and the full test suite. Never push past a
  check whose exit status was hidden, e.g. piped into `tail`.
- After pushing, watch CI until it's green.

**PR descriptions** follow one shape:

- `Fixes #N` first.
- What changed and why, citing spec sections and ROOT source lines.
- A **Tests** section: what each test pins, which fail on `main`, and the
  full-suite count.
- A **Breaking changes** list, if any.

**Marking AI work:**

- Issues, PR descriptions and comments start with `> 🤖 AI generated content`.
- Issue bodies and PR descriptions also end with
  `Assisted-by: <tool>:<model id>`.
- Commit messages end with that same `Assisted-by:` line.
- Text a person writes themselves carries neither mark.
- In a guided PR (below), the agent's scaffolding and the person's code go in
  separate commits, so each is marked correctly: the agent's commits end with
  the `Assisted-by:` line, the person's carry none.

**Guided PRs.** A person may write a PR's code themselves with an agent guiding,
to learn (they say so, or their own instructions do). Then the agent plans the
steps, writes the issue's tests and a skeleton (signatures, types, docstrings,
`TODO`s), explains what each step needs, and reviews what the person writes. It
does not write the implementation unless asked for a specific piece, and does
not fix the person's code for them, CI failures included. Everything else in
this file applies as written.

**Review and merge.**

- Samantha reviews every PR on GitHub; Nick also reviews the model's revision
  (#10) and the `layoutviz.py` PRs (#18, #19). Nothing merges until Samantha
  approves; then it is squash-merged with GitHub's default message.
- Some issues name a review before the next stage (for example, records of a few
  fixtures shown before the next PR starts). Work that depends on it waits.
- Answer reviewer comments in their thread, with the details.
- When a reviewer proposes a design, implement it. If you depart from it, say
  where and why in the reply and in the description.
- When the answer to a question decides the design, ask it in the PR before
  writing the code.
