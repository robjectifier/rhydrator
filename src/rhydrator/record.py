"""The versioned MessagePack record used to reconstruct one input ROOT file."""

import hashlib
import zlib
from pathlib import Path
from typing import TYPE_CHECKING

import msgpack  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from os import PathLike

FORMAT_VERSION = 1


def pack_record(
    source_path: str | PathLike[str],
    file_size: int,
    extents: list[dict[str, int]],
    non_page_bytes: bytearray,
) -> bytes:
    """Pack the source file's checksums, page placements and remaining bytes."""
    if len(non_page_bytes) + sum(extent["length"] for extent in extents) != file_size:
        msg = f"{source_path}: page and non-page bytes do not cover the original file"
        raise ValueError(msg)

    adler32 = 1
    md5 = hashlib.md5(usedforsecurity=False)
    with Path(source_path).open("rb") as source:
        while chunk := source.read(1024 * 1024):
            adler32 = zlib.adler32(chunk, adler32)
            md5.update(chunk)
        if source.tell() != file_size:
            msg = f"{source_path}: file size changed while computing the checksums"
            raise ValueError(msg)

    return msgpack.packb(
        {
            "format_version": FORMAT_VERSION,
            "file_size": file_size,
            "adler32": adler32,
            "md5": md5.digest(),
            "extents": extents,
            "non_page_bytes": non_page_bytes,
        },
        use_bin_type=True,
    )


def unpack_record(data: bytes) -> dict[str, object]:
    """Decode a rehydration record whose format version this reader supports."""
    record = msgpack.unpackb(data, raw=False)
    if not isinstance(record, dict):
        msg = "Expected a MessagePack map for the rehydration record"
        raise ValueError(msg)  # noqa: TRY004 (malformed serialized data)
    version = record.get("format_version")
    if type(version) is not int or version != FORMAT_VERSION:
        msg = f"Unsupported rehydration record format version: {version!r}"
        raise ValueError(msg)
    return record
