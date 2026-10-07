"""The RNTuple files the tests read, from one place (#8).

- root-io-spec's RNTuple fixtures, from the submodule ``reference/root-io-spec``
  (pinned at d1618ad). The submodule is part of the repository, so a missing
  file fails the test.

Test modules take these through the fixtures below. ``test_fixtures.py`` also
imports the plain functions, to test them directly.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT_IO_SPEC_DIR = Path(__file__).parent.parent / "reference" / "root-io-spec"
"""The root-io-spec submodule's checkout."""

ROOT_IO_SPEC_FILES: tuple[str, ...] = (
    "anchor.root",
    "attributes.root",
    "collections.root",
    "compressed.root",
    "fundamental-types.root",
    "map.root",
    "projected.root",
    "soa.root",
    "streamed.root",
    "untyped.root",
    "user-class.root",
)
"""The RNTuple fixtures in root-io-spec's ``data/rntuple/``, by file name."""


def root_io_spec_path(name: str) -> Path:
    """Return the path of root-io-spec's fixture called *name*.

    *name* is a file name such as ``"anchor.root"``; the file lives in
    ``data/rntuple/`` under ``ROOT_IO_SPEC_DIR``.

    The submodule is part of the repository, so a missing file means the
    checkout is incomplete: fail the calling test with ``pytest.fail``, with a
    message that names the missing path and says how to fetch it
    (``git submodule update --init reference/root-io-spec``). Never skip.
    """
    raise NotImplementedError


@pytest.fixture(params=ROOT_IO_SPEC_FILES)
def root_io_spec_file(request: pytest.FixtureRequest) -> Path:
    """Each of root-io-spec's fixtures in turn, as a path.

    A test that takes this fixture runs once per file in
    ``ROOT_IO_SPEC_FILES``, and pytest names each run after the file, e.g.
    ``test_x[anchor.root]``.
    """
    raise NotImplementedError
