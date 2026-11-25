"""Read metadata and stored page bytes with rootfilespec."""

from contextlib import contextmanager
from typing import TYPE_CHECKING

from rootfilespec import reader
from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.bootstrap.TDatime import TDatime_to_datetime
from rootfilespec.rntuple.RNTuple import InterpretableColumnRange, InterpretablePage

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    from uuid import UUID

    from rootfilespec.rntuple.RNTuple import (
        InterpretableCluster,
        InterpretableColumn,
        RNTuple,
    )

__all__ = [
    "InterpretableColumnRange",
    "InterpretablePage",
    "open_input",
    "page_bytes",
    "page_range",
    "read_bytes",
    "rntuples",
]


@contextmanager
def open_input(
    path: str,
) -> Iterator[tuple[reader.FileReader, datetime | None, UUID | None]]:
    with reader.open_path(path) as source:
        directory = source.rootdir
        uuid = directory.fUUID.fUUID if directory.fUUID is not None else None
        yield source, TDatime_to_datetime(directory.header.fDatimeC), uuid


def rntuples(
    source: reader.FileReader, path: str
) -> Iterator[
    tuple[RNTuple, list[InterpretableColumn], Iterator[InterpretableCluster]]
]:
    for key in source.keylist().fKeys:
        anchor = source.fetch(key)
        if not isinstance(anchor, ROOT3a3aRNTuple):
            msg = f"{path}: expected an RNTuple anchor, got {type(anchor).__name__}"
            raise TypeError(msg)
        ntuple = source.fetch.rntuple(anchor)
        yield ntuple, ntuple.columns(), ntuple.clusters()


def page_range(page: InterpretablePage) -> tuple[int, int]:
    locator = page.pageDescription.page_locator
    return locator.offset, locator.size


def page_bytes(source: reader.FileReader, page: InterpretablePage) -> memoryview:
    """Verify the page checksum and retain the original stored bytes."""
    locator = page.pageDescription.page_locator
    raw = source.fetch.buffer(locator)
    locator.read_from(raw)
    return raw.data


def read_bytes(source: reader.FileReader, offset: int, size: int) -> memoryview:
    return source.fetch.buffer_at(offset, size).data
