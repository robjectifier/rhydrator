"""The RNTuple files the tests read, from one place (#8).

- root-io-spec's RNTuple fixtures, from the submodule ``reference/root-io-spec``
  (pinned at d1618ad). The submodule is part of the repository, so a missing
  file fails the test.
- scikit-hep-testdata's RNTuple files, through ``skhep_testdata.data_path``,
  which downloads them once into its cache: every RNTuple file the installed
  release knows. A test skips, with the reason, when a download fails.
- The three large CMS files, from the directory named by the environment
  variable ``RHYDRATOR_LARGE_FILES`` (the README, *Running the tests*). Not in
  the repository, never in CI. A test skips when the variable is unset or
  empty; when it is set, a missing file, or one of another size, fails, so a
  wrong path cannot pass as a skip.

Test modules take these through the fixtures below. ``test_fixtures.py`` also
imports the plain functions, to test them directly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import skhep_testdata

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

    Fails the calling test, never skips it, when the file is missing: the
    submodule is part of the repository, so the checkout is incomplete.
    """
    path = ROOT_IO_SPEC_DIR / "data" / "rntuple" / name
    if not path.is_file():
        pytest.fail(
            f"{path} is missing; fetch it with "
            "`git submodule update --init reference/root-io-spec`"
        )
    return path


@pytest.fixture(params=ROOT_IO_SPEC_FILES)
def root_io_spec_file(request: pytest.FixtureRequest) -> Path:
    """Each of root-io-spec's fixtures in turn, as a path.

    A test that takes this fixture runs once per file in
    ``ROOT_IO_SPEC_FILES``, and pytest names each run after the file, e.g.
    ``test_x[anchor.root]``.
    """
    return root_io_spec_path(request.param)


# --- scikit-hep-testdata -----------------------------------------------------

SKHEP_FILES = sorted(n for n in skhep_testdata.known_files if "rntuple" in n)
"""scikit-hep-testdata's RNTuple files, by file name: every one the installed
release knows, so a new release's files are tested without a change here."""


def skhep_path(name: str) -> Path:
    """Return the path of scikit-hep-testdata's file called *name*.

    Skips the calling test when the download fails.
    """
    try:
        return Path(skhep_testdata.data_path(name))
    except OSError as err:  # requests' errors are OSErrors
        pytest.skip(f"scikit-hep-testdata could not fetch {name}: {err}")


@pytest.fixture(params=SKHEP_FILES)
def skhep_file(request: pytest.FixtureRequest) -> Path:
    """Each of scikit-hep-testdata's RNTuple files in turn, as a path."""
    return skhep_path(request.param)


# --- Large files -------------------------------------------------------------

LARGE_FILES_VAR = "RHYDRATOR_LARGE_FILES"
"""The environment variable naming the directory that holds the large files."""

LARGE_FILE_SIZES: dict[str, int] = {
    "DYJetsToLL_RunIIAutumn18NanoAODv7_RNTuple_named_zstd.root": 1_891_767_386,
    "initialconversion_rntuple_mini24_lzma4_buffer4.rntpl": 3_391_001_326,
    "initialconversion_rntuple_minisim24_lzma4_buffer4.rntpl": 5_999_969_177,
}
"""The large CMS files (NanoAOD, MiniAOD, MiniSim) and their sizes in bytes."""


def large_file_path(name: str) -> Path:
    """Return the path of the large file called *name*.

    Skips the calling test when ``RHYDRATOR_LARGE_FILES`` is unset or empty.
    When it is set, fails the test if the file is missing or of another size.
    """
    directory = os.environ.get(LARGE_FILES_VAR)
    if not directory:
        pytest.skip(f"{LARGE_FILES_VAR} is not set (see the README)")
    path = Path(directory).expanduser() / name
    if not path.is_file():
        pytest.fail(f"{path} is missing ({LARGE_FILES_VAR}={directory})")
    size = path.stat().st_size
    if size != LARGE_FILE_SIZES[name]:
        pytest.fail(
            f"{path} is {size} bytes, not {LARGE_FILE_SIZES[name]}: "
            "another file under this name"
        )
    return path


@pytest.fixture(params=sorted(LARGE_FILE_SIZES))
def large_file(request: pytest.FixtureRequest) -> Path:
    """Each of the large files in turn, as a path."""
    return large_file_path(request.param)
