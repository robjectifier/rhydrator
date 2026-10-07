"""Every test file is found, at its pinned size; a missing one never passes (#8)."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import ROOT_IO_SPEC_DIR, ROOT_IO_SPEC_FILES, root_io_spec_path

# --- root-io-spec ------------------------------------------------------------

ROOT_IO_SPEC_SIZES = {
    "anchor.root": 2592,
    "attributes.root": 5443,
    "collections.root": 6076,
    "compressed.root": 1420,
    "fundamental-types.root": 4013,
    "map.root": 4524,
    "projected.root": 3182,
    "soa.root": 3462,
    "streamed.root": 4007,
    "untyped.root": 2838,
    "user-class.root": 6009,
}
"""Sizes in bytes of the files at root-io-spec d1618ad."""


def test_root_io_spec_lists_every_pinned_file():
    assert sorted(ROOT_IO_SPEC_FILES) == sorted(ROOT_IO_SPEC_SIZES)


def test_root_io_spec_file_at_pinned_size(root_io_spec_file: Path):
    assert (
        root_io_spec_file.stat().st_size == ROOT_IO_SPEC_SIZES[root_io_spec_file.name]
    )


def test_root_io_spec_file_is_in_the_repository(root_io_spec_file: Path):
    assert root_io_spec_file.is_relative_to(ROOT_IO_SPEC_DIR / "data" / "rntuple")


def test_root_io_spec_path_finds_a_file():
    assert root_io_spec_path("anchor.root") == (
        ROOT_IO_SPEC_DIR / "data" / "rntuple" / "anchor.root"
    )


def test_root_io_spec_missing_file_fails():
    # pytest.fail raises pytest.fail.Exception; a skip would raise
    # pytest.skip.Exception instead, and this test would fail.
    with pytest.raises(pytest.fail.Exception, match=r"no-such-file\.root"):
        root_io_spec_path("no-such-file.root")


def test_root_io_spec_case_toml_beside_each_file():
    for name in ROOT_IO_SPEC_FILES:
        case = ROOT_IO_SPEC_DIR / "gen" / "cases" / "rntuple" / Path(name).stem
        assert (case / "case.toml").is_file(), case
