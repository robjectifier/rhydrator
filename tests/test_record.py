"""The local rehydration record and its relationship to the stored page bundles."""

import hashlib
import zlib
from pathlib import Path

import msgpack  # type: ignore[import-untyped]
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from conftest import root_io_spec_path
from rhydrator import model as m
from rhydrator.ingest import NewFile, ingest_file
from rhydrator.record import unpack_record


def test_record_reconstructs_map_fixture(tmp_path: Path):
    source = root_io_spec_path("map.root")
    original = source.read_bytes()
    engine = create_engine("sqlite://")
    m.Base.metadata.create_all(engine)
    with Session(engine) as session:
        ingest_file(session, NewFile("maps", source.name, str(source)), objects_dir=tmp_path)
        info = session.scalars(select(m.RehydrationInfo)).one()
        assert info.input_file.lfn == source.name
        assert info.format_version == 1
        packed = (tmp_path / f"rehydration-{info.record_object_id}").read_bytes()

    record = msgpack.unpackb(packed, raw=False)
    assert unpack_record(packed) == record
    assert record["format_version"] == 1
    assert record["file_size"] == len(original)
    assert record["adler32"] == zlib.adler32(original)
    assert record["md5"] == hashlib.md5(original, usedforsecurity=False).digest()

    # map.root's fMap and fMulti columns both reference the page at byte 1798.
    offsets = [extent["file_offset"] for extent in record["extents"]]
    assert offsets == sorted(set(offsets))
    assert offsets.count(1798) == 1
    restored = bytearray(record["non_page_bytes"])
    for extent in record["extents"]:
        bundle = (tmp_path / str(extent["bundle_id"])).read_bytes()
        offset = extent["bundle_offset"]
        data = bundle[offset : offset + extent["length"]]
        file_offset = extent["file_offset"]
        restored[file_offset:file_offset] = data
    assert restored == original


def test_record_collision_preserves_existing_object(tmp_path: Path):
    source = root_io_spec_path("map.root")
    existing = tmp_path / "rehydration-1"
    existing.write_bytes(b"existing object")
    engine = create_engine("sqlite://")
    m.Base.metadata.create_all(engine)
    with Session(engine) as session:
        with pytest.raises(FileExistsError):
            ingest_file(session, NewFile("maps", source.name, str(source)), objects_dir=tmp_path)
        assert session.scalars(select(m.InputFile)).first() is None
        assert session.scalars(select(m.PageBundle)).first() is None
        assert session.scalars(select(m.RehydrationInfo)).first() is None
    assert list(tmp_path.iterdir()) == [existing]
    assert existing.read_bytes() == b"existing object"


def test_unknown_record_version():
    with pytest.raises(ValueError, match="format version: 99"):
        unpack_record(msgpack.packb({"format_version": 99}))
