# root-io-spec's RNTuple fixtures

Vendored from [root-io-spec](https://github.com/ariostas/root-io-spec) at commit
[`d1618ad`](https://github.com/ariostas/root-io-spec/tree/d1618ad96c7e55277b291eb9e3af4ee8d0c359d9)
(release 2026.09.24), keeping root-io-spec's paths:

- `data/rntuple/`: the 11 RNTuple files, written by ROOT 6.40.04;
- `gen/cases/rntuple/<name>/`: each file's `case.toml` (its byte assertions) and
  the generator that wrote it, which root-io-spec's licence asks to travel with
  the files.

They are BSD-3-Clause, copyright Andres Rios Tascon: see `LICENSE`. The files
are copied byte for byte and excluded from pre-commit, so no hook rewrites them.
To update them, copy them again from a newer root-io-spec release and change the
commit here and the pinned sizes in `tests/test_fixtures.py`.
