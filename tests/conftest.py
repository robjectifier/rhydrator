"""The RNTuple files the tests read, from one place (#8).

- root-io-spec's RNTuple fixtures, from the submodule ``reference/root-io-spec``
  (pinned at d1618ad). The submodule is part of the repository, so a missing
  file fails the test.
- scikit-hep-testdata's RNTuple files, through ``skhep_testdata.data_path``,
  which downloads them once into its cache. Optional: a test skips, with the
  reason, when the package is missing or a download fails.
- The three large CMS files, from the directory named by the environment
  variable ``RHYDRATOR_LARGE_FILES`` (the README, *Running the tests*). Not in
  the repository, never in CI. A test skips when the variable is unset or
  empty; when it is set, a missing file, or one of another size, fails, so a
  wrong path cannot pass as a skip.
- Files made at test time from ``anchor.root``, whose envelopes are
  uncompressed: one with bytes after ``fEND``, one with an unknown feature flag
  set and its checksums recomputed.

Test modules take these through the fixtures below. ``test_fixtures.py`` also
imports the plain functions, to test them directly.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path

import pytest
import xxhash

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

SKHEP_FILES: tuple[str, ...] = (
    "Run2012BC_DoubleMuParked_Muons_1000evts_rntuple_v1-0-0-0.root",
    "cmsopendata2015_ttbar_19980_NANOAOD_RNTupleImporter_rntuple_v1-0-0-1.root",
    "ntpl001_staff_rntuple_v1-0-0-0.root",
    "ntpl001_staff_rntuple_v1-0-1-0.root",
    "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root",
    "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root",
    "test_1jag_int_float_rntuple_v1-0-0-0.root",
    "test_atomic_bitset_rntuple_v1-0-0-0.root",
    "test_bit_rntuple_v1-0-0-0.root",
    "test_class_inheritance_rntuple_v1-0-0-1.root",
    "test_emptystruct_invalidvar_rntuple_v1-0-0-0.root",
    "test_extension_columns_rntuple_v1-0-0-0.root",
    "test_float_types_rntuple_v1-0-0-0.root",
    "test_index_multicluster_rntuple_v1-0-0-0.root",
    "test_int_5e4_rntuple_v1-0-0-0.root",
    "test_int_float_rntuple_v1-0-0-0.root",
    "test_int_multicluster_rntuple_v1-0-0-0.root",
    "test_int_vfloat_tlv_vtlv_rntuple_v1-0-0-0.root",
    "test_multiple_cluster_groups_rntuple_v1-0-0-0.root",
    "test_multiple_representations_rntuple_v1-0-0-0.root",
    "test_nested_structs_rntuple_v1-0-0-0.root",
    "test_split_3e4_rntuple_v1-0-0-0.root",
    "test_splitint_rntuple_v1-0-1-0.root",
    "test_stl_containers_rntuple_v1-0-0-0.root",
    "uproot-physlite-rntuple_v1-0-0-0.root",
)
"""scikit-hep-testdata's RNTuple files, by file name."""


def skhep_path(name: str) -> Path:
    """Return the path of scikit-hep-testdata's file called *name*.

    Skips the calling test when the package is missing or the download fails.
    A name the package doesn't know raises ``FileNotFoundError``: that is a
    mistake in ``SKHEP_FILES``, not an unavailable source.
    """
    skhep_testdata = pytest.importorskip("skhep_testdata")
    try:
        return Path(skhep_testdata.data_path(name))
    except FileNotFoundError:
        raise
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
    "DYJetsToLL_RunIIAutumn18NanoAODv7_RNTuple.root": 1_818_127_396,
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


# --- Files made at test time -------------------------------------------------

# Offsets in anchor.root, from its case.toml and the anchor's own fields: the
# header envelope at 268 (240 bytes) and the footer envelope at 838 (160
# bytes). Each envelope ends with the XXH3-64 of everything before it,
# little-endian (spec, Envelopes); each begins with an 8-byte type and length,
# then the feature flags (spec, Header Envelope and Footer Envelope). The
# footer's next 8 bytes repeat the header's checksum.
ANCHOR_HEADER = (268, 240)
ANCHOR_FOOTER = (838, 160)
ANCHOR_FEND_OFFSET = 12
"""Where the file header's 32-bit ``fEND`` is (big-endian, a small file)."""

TRAILING_BYTES = b"bytes after fEND"

UNKNOWN_FEATURE_FLAG = 1 << 62
"""A feature flag no RNTuple version defines: bit 62, the highest bit that
doesn't mean more flags follow (spec, Feature Flags)."""


def envelope_checksum(data: bytes | bytearray, seek: int, length: int) -> bytes:
    """The XXH3-64 checksum of the envelope at *seek*, as stored."""
    return struct.pack("<Q", xxhash.xxh3_64_intdigest(data[seek : seek + length - 8]))


def with_bytes_after_end(source: Path) -> bytes:
    """*source*'s bytes, followed by ``TRAILING_BYTES``; ``fEND`` unchanged."""
    return source.read_bytes() + TRAILING_BYTES


def with_feature_flag(source: Path, flag: int = UNKNOWN_FEATURE_FLAG) -> bytes:
    """``anchor.root``'s bytes with *flag* set in the header's feature flags.

    The header's checksum is recomputed, and so are the footer's copy of it and
    the footer's own checksum, so only the flag makes the file unreadable.
    """
    data = bytearray(source.read_bytes())
    header_seek, header_len = ANCHOR_HEADER
    footer_seek, footer_len = ANCHOR_FOOTER
    flags_at = header_seek + 8
    (flags,) = struct.unpack_from("<q", data, flags_at)
    struct.pack_into("<q", data, flags_at, flags | flag)
    header_checksum = envelope_checksum(data, header_seek, header_len)
    data[header_seek + header_len - 8 : header_seek + header_len] = header_checksum
    data[footer_seek + 16 : footer_seek + 24] = header_checksum
    footer_checksum = envelope_checksum(data, footer_seek, footer_len)
    data[footer_seek + footer_len - 8 : footer_seek + footer_len] = footer_checksum
    return bytes(data)


@pytest.fixture
def file_with_bytes_after_end(tmp_path: Path) -> Path:
    """``anchor.root`` with bytes after ``fEND``, written to a temporary file."""
    path = tmp_path / "bytes-after-fend.root"
    path.write_bytes(with_bytes_after_end(root_io_spec_path("anchor.root")))
    return path


@pytest.fixture
def file_with_feature_flag(tmp_path: Path) -> Path:
    """``anchor.root`` with an unknown feature flag set, written to a temporary
    file."""
    path = tmp_path / "feature-flag.root"
    path.write_bytes(with_feature_flag(root_io_spec_path("anchor.root")))
    return path
