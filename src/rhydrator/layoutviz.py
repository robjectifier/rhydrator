"""Visualize file layout using speedscope file format

Schema doc: https://github.com/jlfwong/speedscope/blob/main/src/lib/file-format-spec.ts
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOTFile
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.rntuple.RNTuple import RNTuple, SchemaDescription
from rootfilespec.serializable import BufferContext, ReadBuffer


class PageData(TypedDict):
    cluster: int
    offset: int
    size: int
    elements: int


class ColumnData(TypedDict):
    id: int
    type: str
    pages: list[PageData]


FieldColumnMap: TypeAlias = defaultdict[int, list[ColumnData]]


class Frame(TypedDict):
    name: str
    file: str | None
    line: int | None
    col: int | None


class Event(TypedDict):
    type: Literal["O", "C"]
    "Open or close event"
    frame: int
    "Frame index"
    at: int
    "Offset in file"


class Span(TypedDict):
    offset: int
    size: int
    stack: list[int]


@dataclass
class ProfileBulder:
    frames: list[Frame] = field(default_factory=list)
    "All frames in the profile"
    spans: list[Span] = field(default_factory=list)
    "All byte range spans in the file"
    stack: list[int] = field(default_factory=list)
    "Current stack of frame indices"
    shared_frames: dict[tuple[str, str | None], int] = field(default_factory=dict)
    "Frames where the name is treated as unique symbol identifier"

    def push_frame(self, frame: Frame):
        self.frames.append(frame)
        self.stack.append(len(self.frames) - 1)

    def pop_frame(self):
        self.stack.pop()

    def push_shared_frame(self, name: str, file: str | None = None):
        self.stack.append(self.shared_frame_id(name, file))

    def shared_frame_id(self, name: str, file: str | None) -> int:
        """Get or create a frame ID for the given shared frame"""
        key = (name, file)
        if key not in self.shared_frames:
            frame: Frame = {
                "name": name,
                "file": file,
                "line": None,
                "col": None,
            }
            self.frames.append(frame)
            self.shared_frames[key] = len(self.frames) - 1
        return self.shared_frames[key]

    def add_span(self, name: str, offset: int, size: int):
        """Add a span of bytes in the file to the profile

        Args:
            name (str): Name of the span
            offset (int): Offset in the file
            size (int): Size of the span
        """
        self.push_shared_frame(name)
        self.spans.append(
            {
                "offset": offset,
                "size": size,
                "stack": self.stack.copy(),
            }
        )
        self.stack.pop()

    def render(self, endValue: int) -> dict:
        """Render the profile as a dictionary suitable for JSON serialization

        Returns:
            dict: Profile dictionary
        """
        assert not self.stack, "Profile stack is not empty"
        events: list[Event] = []

        stack: list[int] = []
        last_span_end = 0
        gaps: list[tuple[int, int]] = []
        for span in sorted(self.spans, key=lambda s: s["offset"]):
            if span["offset"] < last_span_end:
                msg = f"Overlapping spans detected: {span=} starts before last span ended at {last_span_end}"
                sstack = " > ".join(repr(self.frames[frame]) for frame in span["stack"])
                msg += f"\nSpan stack: {sstack}"
                cstack = " > ".join(repr(self.frames[frame]) for frame in stack)
                msg += f"\nCurrent stack: {cstack}"
                raise ValueError(msg)
            if span["offset"] > last_span_end:
                gaps.append((last_span_end, span["offset"] - last_span_end))
            if stack:
                # always close frames down to common ancestor
                common = [
                    i
                    for i, (left, right) in enumerate(
                        zip(stack, span["stack"], strict=False)
                    )
                    if left != right
                ]
                # and always close the last frame (last span must have ended)
                icommon = min(common) if common else len(stack) - 1
                stack, closing = stack[:icommon], stack[icommon:]
                events.extend(
                    {"type": "C", "frame": frame, "at": last_span_end}
                    for frame in reversed(closing)
                )
                opening = span["stack"][icommon:]
            else:
                opening = span["stack"]
            events.extend(
                {"type": "O", "frame": frame, "at": span["offset"]} for frame in opening
            )
            stack.extend(opening)
            last_span_end = span["offset"] + span["size"]

        events.extend(
            {"type": "C", "frame": frame, "at": last_span_end}
            for frame in reversed(stack)
        )

        # most of these will be the 8-byte checksums after each envelope
        # print(f"Detected {len(gaps)} gaps in file layout totaling {sum(size for _, size in gaps)} of {endValue} bytes")

        opened, closed = 0, 0
        offset = 0
        for i, event in enumerate(events):
            if event["type"] == "O":
                opened += 1
            else:
                closed += 1
            assert event["at"] >= offset, (
                f"Events are out of order: {event=}, previous {events[i - 1]}"
            )
            offset = max(offset, event["at"])
        assert opened == closed, "Unmatched open/close events"

        profile = {
            "type": "evented",
            "name": "RNTuple File Layout",
            "unit": "bytes",
            "startValue": 0,
            "endValue": endValue,
            "events": events,
        }
        return {
            "$schema": "https://www.speedscope.app/file-format-schema.json",
            "shared": {
                "frames": self.frames,
            },
            "profiles": [profile],
            "exporter": "rhydrator.layoutviz",
        }


UNIQUE_FIELDS = False
UNIQUE_COLUMNS = False


def descend(
    profile: ProfileBulder,
    schema: SchemaDescription,
    fieldColumns: FieldColumnMap,
    fieldID: int,
):
    fieldDescription = schema.fieldDescriptions[fieldID]
    # TODO: fFlags should be an IntFlag (like TObjFlag)
    if fieldDescription.fFlags & 0x2:
        # skip projected fields
        return
    field_name = fieldDescription.fFieldName.fString.decode()
    field_type = fieldDescription.fTypeName.fString.decode()
    if UNIQUE_FIELDS:
        profile.push_frame(
            {
                "name": f"Field {fieldID}: {field_name}",
                "file": f"{field_type}",
                "line": None,
                "col": None,
            },
        )
    else:
        profile.push_shared_frame(field_name, field_type)
    for column in fieldColumns.get(fieldID, []):
        if UNIQUE_COLUMNS:
            profile.push_frame(
                {
                    "name": f"Column {column['id']}: {column['type']}",
                    "file": None,
                    "line": None,
                    "col": None,
                },
            )
        else:
            profile.push_shared_frame(f"Column {column['type']}")
        for page in column["pages"]:
            profile.add_span(
                "Page",  # (cluster {page['cluster']})
                offset=page["offset"],
                size=page["size"],
            )
        profile.pop_frame()

    for childID, childDescription in enumerate(schema.fieldDescriptions):
        if childDescription.fParentFieldID != fieldID or childID == fieldID:
            continue
        descend(profile, schema, fieldColumns, childID)
    profile.pop_frame()


def read(path: Path):
    profile = ProfileBulder()
    profile.push_frame(
        {
            "name": str(path),
            "file": None,
            "line": None,
            "col": None,
        }
    )
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(
                memoryview(filehandle.read(size)),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=seek),
            )

        # Get TFile Header
        buffer = fetch_data(0, 512)
        file, _ = ROOTFile.read(buffer)
        profile.add_span(
            "ROOTFile",
            offset=0,
            size=file.header.fBEGIN,
        )

        def fetch_cached(seek: int, size: int):
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            return fetch_data(seek, size)

        # Get TFile object (root TDirectory)
        tfile = file.get_TFile(fetch_cached)

        # usually the directory tkeylist and the streamer info are adjacent at the end of the file
        profile.add_span(
            "TKeyList",
            offset=tfile.rootdir.fSeekKeys,
            size=tfile.rootdir.header.fNbytesKeys,
        )
        profile.add_span(
            "TStreamerInfo",
            offset=file.header.fSeekInfo,
            size=file.header.fNbytesInfo,
        )

        # Get TKeyList (List of all TKeys in the TDirectory)
        keylist = tfile.get_KeyList(fetch_data)

        #### Get RNTuple Info
        # Only RNTuple Anchor TKeys are visible (i.e. in TKeyList); ClassName = ROOT::RNTuple
        for name, tkey in keylist.items():
            profile.add_span(
                f"{name}: {tkey.fClassName.fString.decode()}",
                offset=tkey.fSeekKey,
                size=tkey.header.fNbytes,
            )
            # Check for RNTuple Anchors
            if tkey.fClassName.fString != b"ROOT::RNTuple":
                continue
            ### Get RNTuple Anchor Object
            anchor = tkey.read_object(fetch_data, ROOT3a3aRNTuple)

            profile.push_frame(
                {
                    "name": f"RNTuple: {name}",
                    "file": None,
                    "line": None,
                    "col": None,
                }
            )

            profile.add_span(
                "HeaderEnvelope",
                offset=anchor.fSeekHeader,
                size=anchor.fNBytesHeader,
            )
            profile.add_span(
                "FooterEnvelope",
                offset=anchor.fSeekFooter,
                size=anchor.fNBytesFooter,
            )

            rntuple = RNTuple.from_anchor(anchor, fetch_data)
            for cg in rntuple.footerEnvelope.clusterGroups:
                link = cg.pagelistLink
                profile.add_span(
                    "PageListEnvelope",
                    # TODO: add local_offset() to base class in rootfilespec
                    offset=link.locator.offset,  # type: ignore[attr-defined]
                    size=link.locator.size,
                )

            schemaDescription = rntuple.schemaDescription

            columnPages: defaultdict[int, list[PageData]] = defaultdict(list)
            seen_pages: set[tuple[int, int]] = set()
            page2column: defaultdict[tuple[int, int, int], set[int]] = defaultdict(set)

            for ple in rntuple.pagelistEnvelopes:
                # ple.pageLocations is [cluster][column][page]
                for clusterId, pc in enumerate(ple.pageLocations):
                    for columnId, pl in enumerate(pc):
                        for page in pl:
                            tup = (page.locator.offset, page.locator.size)  # type: ignore[attr-defined]
                            page2column[tup[0], tup[1], clusterId].add(columnId)
                            if tup in seen_pages:
                                continue
                            seen_pages.add(tup)
                            columnPages[columnId].append(
                                {
                                    "cluster": clusterId,
                                    "offset": page.locator.offset,  # type: ignore[attr-defined]
                                    "size": page.locator.size,
                                    "elements": page.fNElements,
                                }
                            )

            # for page, columns in page2column.items():
            #     if len(columns) > 1:
            #         print(
            #             f"Page in cluster {page[2]} at offset {page[0]} size {page[1]} belongs to multiple columns: {columns}"
            #         )

            fieldColumns: FieldColumnMap = defaultdict(list)
            for columnId, columnDescription in enumerate(
                schemaDescription.columnDescriptions
            ):
                ctype = repr(columnDescription.fColumnType).removeprefix("ColumnType.")
                fieldColumns[columnDescription.fFieldID].append(
                    {
                        "id": columnId,
                        "type": ctype,
                        "pages": columnPages.get(columnId, []),
                    }
                )

            pagedata: list[tuple[str, int, int]] = []
            for columnId, columnDescription in enumerate(
                schemaDescription.columnDescriptions
            ):
                ctype = repr(columnDescription.fColumnType).removeprefix("ColumnType.")
                pagedata.extend(
                    (ctype, page["size"], page["elements"])
                    for page in columnPages.get(columnId, [])
                )
            with Path(f"pagedata_{name}.csv").open("w") as pagedatafile:
                pagedatafile.write("ColumnType,PageSize,NumElements\n")
                for ctype, size, elements in pagedata:
                    pagedatafile.write(f"{ctype},{size},{elements}\n")

            topLevelFields = [
                fieldID
                for fieldID, fieldDescription in enumerate(
                    schemaDescription.fieldDescriptions
                )
                if fieldDescription.fParentFieldID == fieldID
            ]
            for fieldID in topLevelFields:
                descend(profile, schemaDescription, fieldColumns, fieldID)
            profile.pop_frame()

        profile.pop_frame()
        filehandle.seek(0, 2)
        file_size = filehandle.tell()

    out = profile.render(endValue=file_size)
    with gzip.open(path.with_suffix(".layout.json.gz"), "wt") as outfile:
        json.dump(out, outfile)


def main():
    parser = argparse.ArgumentParser(
        description="Visualize ROOT file layout using speedscope format"
    )
    parser.add_argument(
        "filename", type=Path, help="Path to the ROOT file to visualize"
    )
    args = parser.parse_args()
    read(args.filename)
