"""Every test file is found, at its pinned size; a missing one never passes (#8)."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest
from conftest import (
    ANCHOR_FEND_OFFSET,
    ANCHOR_FOOTER,
    ANCHOR_HEADER,
    LARGE_FILE_SIZES,
    LARGE_FILES_VAR,
    ROOT_IO_SPEC_DIR,
    ROOT_IO_SPEC_FILES,
    SKHEP_FILES,
    TRAILING_BYTES,
    UNKNOWN_FEATURE_FLAG,
    envelope_checksum,
    large_file_path,
    root_io_spec_path,
    skhep_path,
)

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


# --- scikit-hep-testdata -----------------------------------------------------

SKHEP_SIZES = {
    "Run2012BC_DoubleMuParked_Muons_1000evts_rntuple_v1-0-0-0.root": 27643,
    "cmsopendata2015_ttbar_19980_NANOAOD_RNTupleImporter_rntuple_v1-0-0-1.root": 50467,
    "ntpl001_staff_rntuple_v1-0-0-0.root": 25267,
    "ntpl001_staff_rntuple_v1-0-1-0.root": 25318,
    "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root": 2382,
    "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root": 2514,
    "test_1jag_int_float_rntuple_v1-0-0-0.root": 2278,
    "test_atomic_bitset_rntuple_v1-0-0-0.root": 1561,
    "test_bit_rntuple_v1-0-0-0.root": 1390,
    "test_class_inheritance_rntuple_v1-0-0-1.root": 3122,
    "test_emptystruct_invalidvar_rntuple_v1-0-0-0.root": 1811,
    "test_extension_columns_rntuple_v1-0-0-0.root": 3776,
    "test_float_types_rntuple_v1-0-0-0.root": 1890,
    "test_index_multicluster_rntuple_v1-0-0-0.root": 2353,
    "test_int_5e4_rntuple_v1-0-0-0.root": 2255,
    "test_int_float_rntuple_v1-0-0-0.root": 1561,
    "test_int_multicluster_rntuple_v1-0-0-0.root": 1765,
    "test_int_vfloat_tlv_vtlv_rntuple_v1-0-0-0.root": 2090,
    "test_multiple_cluster_groups_rntuple_v1-0-0-0.root": 7162,
    "test_multiple_representations_rntuple_v1-0-0-0.root": 1724,
    "test_nested_structs_rntuple_v1-0-0-0.root": 2102,
    "test_split_3e4_rntuple_v1-0-0-0.root": 1780,
    "test_splitint_rntuple_v1-0-1-0.root": 1629,
    "test_stl_containers_rntuple_v1-0-0-0.root": 3000,
    "uproot-physlite-rntuple_v1-0-0-0.root": 2072650,
}
"""Sizes in bytes of the files in scikit-hep-testdata 0.6.9."""


def test_skhep_lists_every_pinned_file():
    assert sorted(SKHEP_FILES) == sorted(SKHEP_SIZES)


def test_skhep_file_at_pinned_size(skhep_file: Path):
    assert skhep_file.stat().st_size == SKHEP_SIZES[skhep_file.name]


def test_skhep_failed_download_skips(monkeypatch: pytest.MonkeyPatch):
    skhep_testdata = pytest.importorskip("skhep_testdata")

    def offline(*_args: object) -> str:
        msg = "offline"
        raise ConnectionError(msg)

    monkeypatch.setattr(skhep_testdata, "data_path", offline)
    with pytest.raises(pytest.skip.Exception, match="offline"):
        skhep_path("test_bit_rntuple_v1-0-0-0.root")


def test_skhep_unknown_file_is_an_error():
    pytest.importorskip("skhep_testdata")
    with pytest.raises(FileNotFoundError):
        skhep_path("no-such-file.root")


# --- Large files -------------------------------------------------------------

LARGE_NAME = "DYJetsToLL_RunIIAutumn18NanoAODv7_RNTuple.root"


def test_large_file_at_pinned_size(large_file: Path):
    assert large_file.stat().st_size == LARGE_FILE_SIZES[large_file.name]


@pytest.mark.parametrize("value", [None, ""])
def test_large_files_unset_skips(monkeypatch: pytest.MonkeyPatch, value):
    if value is None:
        monkeypatch.delenv(LARGE_FILES_VAR, raising=False)
    else:
        monkeypatch.setenv(LARGE_FILES_VAR, value)
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


def test_large_file_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    # A sparse file of the pinned size stands in for the real one.
    with (tmp_path / LARGE_NAME).open("wb") as f:
        f.truncate(LARGE_FILE_SIZES[LARGE_NAME])
    monkeypatch.setenv(LARGE_FILES_VAR, str(tmp_path))
    assert large_file_path(LARGE_NAME) == tmp_path / LARGE_NAME


# --- Files made at test time -------------------------------------------------


def test_anchor_offsets_match_the_anchor():
    data = root_io_spec_path("anchor.root").read_bytes()
    # The anchor's fields after its version (ERRATA 2): seek, compressed and
    # uncompressed length of the header, then of the footer; big-endian.
    header_seek, header_len = ANCHOR_HEADER
    footer_seek, footer_len = ANCHOR_FOOTER
    assert struct.unpack_from(">6Q", data, 1058 + 8) == (
        header_seek,
        header_len,
        header_len,
        footer_seek,
        footer_len,
        footer_len,
    )
    for seek, length in (ANCHOR_HEADER, ANCHOR_FOOTER):
        assert data[seek + length - 8 : seek + length] == envelope_checksum(
            data, seek, length
        )


def test_bytes_after_end(file_with_bytes_after_end: Path):
    anchor = root_io_spec_path("anchor.root").read_bytes()
    data = file_with_bytes_after_end.read_bytes()
    assert data == anchor + TRAILING_BYTES
    (fend,) = struct.unpack_from(">i", data, ANCHOR_FEND_OFFSET)
    assert fend == len(anchor) < len(data)


def test_feature_flag_set_and_checksums_verify(file_with_feature_flag: Path):
    data = file_with_feature_flag.read_bytes()
    header_seek, header_len = ANCHOR_HEADER
    footer_seek, footer_len = ANCHOR_FOOTER
    assert struct.unpack_from("<q", data, header_seek + 8) == (UNKNOWN_FEATURE_FLAG,)
    header_checksum = data[header_seek + header_len - 8 : header_seek + header_len]
    assert header_checksum == envelope_checksum(data, header_seek, header_len)
    assert data[footer_seek + 16 : footer_seek + 24] == header_checksum
    assert data[footer_seek + footer_len - 8 : footer_seek + footer_len] == (
        envelope_checksum(data, footer_seek, footer_len)
    )


def test_feature_flag_changes_only_the_flag_and_checksums(
    file_with_feature_flag: Path,
):
    anchor = root_io_spec_path("anchor.root").read_bytes()
    data = file_with_feature_flag.read_bytes()
    assert len(data) == len(anchor)
    changed = {i for i, (a, b) in enumerate(zip(anchor, data, strict=True)) if a != b}
    # The flags at 276, the header checksum at 500, its copy in the footer at
    # 854 and the footer checksum at 990 (anchor's case.toml pins 276, 500, 854).
    allowed = set().union(*(range(at, at + 8) for at in (276, 500, 854, 990)))
    assert changed
    assert changed <= allowed
