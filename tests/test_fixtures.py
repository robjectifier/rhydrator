"""The test files are found; a missing one never passes (#8)."""

from typing import TYPE_CHECKING

import pytest
import skhep_testdata
from conftest import LARGE_FILES_VAR, large_file_path, root_io_spec_path, skhep_path

if TYPE_CHECKING:
    from pathlib import Path

# --- root-io-spec ------------------------------------------------------------


def test_root_io_spec_file_exists(root_io_spec_file: Path):
    assert root_io_spec_file.is_file()


def test_root_io_spec_missing_file_fails():
    # A skip would raise pytest.skip.Exception instead, and this test would fail.
    with pytest.raises(pytest.fail.Exception, match=r"no-such-file\.root"):
        root_io_spec_path("no-such-file.root")


# --- scikit-hep-testdata -----------------------------------------------------


def test_skhep_file_exists(skhep_file: Path):
    assert skhep_file.is_file()


def test_skhep_failed_download_skips(monkeypatch: pytest.MonkeyPatch):
    def offline(*_args: object) -> str:
        msg = "offline"
        raise ConnectionError(msg)

    monkeypatch.setattr(skhep_testdata, "data_path", offline)
    with pytest.raises(pytest.skip.Exception, match="offline"):
        skhep_path("test_bit_rntuple_v1-0-0-0.root")


# --- Large files -------------------------------------------------------------

LARGE_NAME = "DYJetsToLL_RunIIAutumn18NanoAODv7_RNTuple_named_zstd.root"


def test_large_files_unset_skips(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(LARGE_FILES_VAR, raising=False)
    with pytest.raises(pytest.skip.Exception, match=LARGE_FILES_VAR):
        large_file_path(LARGE_NAME)


def test_large_file_missing_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv(LARGE_FILES_VAR, str(tmp_path))
    with pytest.raises(pytest.fail.Exception, match="is missing"):
        large_file_path(LARGE_NAME)


def test_large_file_of_another_size_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    (tmp_path / LARGE_NAME).write_bytes(b"not the file")
    monkeypatch.setenv(LARGE_FILES_VAR, str(tmp_path))
    with pytest.raises(pytest.fail.Exception, match="12 bytes"):
        large_file_path(LARGE_NAME)
