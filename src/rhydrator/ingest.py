"""Ingestion module for RHydrator."""

import hashlib
import pickle
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select

from . import model as m
from . import reader
from .bundle import Bundler, one_page_per_object
from .record import FORMAT_VERSION, pack_record

if TYPE_CHECKING:
    from datetime import datetime
    from os import PathLike

    from sqlalchemy.orm import Session


@dataclass
class NewFile:
    dataset_name: str
    file_lfn: str
    file_path: str


def get_dataset(
    session: Session, dataset_name: str, dataset_created_at: datetime | None
) -> m.Dataset:
    """Find the dataset by name or create it for its first input file."""

    dataset = session.scalars(
        select(m.Dataset).where(m.Dataset.name == dataset_name)
    ).one_or_none()
    if dataset is None:
        dataset = m.Dataset(name=dataset_name, created_at=dataset_created_at)
        session.add(dataset)
        session.flush()
    return dataset


def _write_object(path: Path, data: bytes | bytearray, cleanup: ExitStack) -> None:
    """Remove this ingest's new object if writing or committing the file fails."""
    with path.open("xb") as object_file:
        cleanup.callback(path.unlink)
        object_file.write(data)


def ingest_file(
    session: Session,
    file: NewFile,
    *,
    objects_dir: str | PathLike[str],
    bundler: Bundler = one_page_per_object,
) -> None:
    """Write the page bundles and rehydration record, then commit the file metadata."""

    object_directory = Path(objects_dir)
    with (
        reader.open_input(file.file_path) as (f, created_at, uuid),
        ExitStack() as cleanup,
    ):
        cleanup.callback(session.rollback)

        m_dataset = get_dataset(session, file.dataset_name, created_at)
        m_input_file = m.InputFile(
            lfn=file.file_lfn,
            uuid=uuid,
        )
        m_dataset.input_files.append(m_input_file)

        placements: dict[tuple[int, int], tuple[m.PageBundle, int]] = {}
        for source_rntuple, columns, clusters in reader.rntuples(f, file.file_path):
            header_envelope = source_rntuple.headerEnvelope
            footer_envelope = source_rntuple.footerEnvelope
            schema = source_rntuple.schemaDescription
            schema_hash = hashlib.md5(
                pickle.dumps(schema, protocol=5), usedforsecurity=False
            ).hexdigest()

            m_rntuple = m.RNTuple(
                name=header_envelope.fName,
                description=header_envelope.fDescription,
                schema_hash=schema_hash,
            )
            m_dataset.rntuples.append(m_rntuple)

            fields_by_source_id: list[m.Field] = []
            for field in schema.fieldDescriptions:
                m_field = m.Field(
                    version=field.fFieldVersion,
                    type_version=field.fTypeVersion,
                    name=field.fFieldName,
                    type_name=field.fTypeName,
                    type_alias=field.fTypeAlias,
                    description=field.fFieldDescription,
                    array_size=field.fArraySize,
                    type_checksum=field.fTypeChecksum,
                )
                m_rntuple.fields.append(m_field)
                fields_by_source_id.append(m_field)

            for _field_id, _field in enumerate(schema.fieldDescriptions):
                parent_field_id = _field.fParentFieldID
                if parent_field_id != _field_id:
                    child = fields_by_source_id[_field_id]
                    child.parent = fields_by_source_id[parent_field_id]

            columns_by_field_and_index: dict[tuple[int, int], m.Column] = {}
            representations_by_physical_id: dict[int, m.ColumnRepresentation] = {}
            for column in columns:
                key = (column.columnDescription.fFieldID, column.index)

                m_column = columns_by_field_and_index.get(key)
                if m_column is None:
                    m_column = m.Column()
                    m_field = fields_by_source_id[column.columnDescription.fFieldID]
                    m_field.columns.append(m_column)
                    columns_by_field_and_index[key] = m_column

                m_representation = m.ColumnRepresentation(
                    column_type=column.columnDescription.fColumnType.value,
                    bits_on_storage=column.columnDescription.fBitsOnStorage,
                    first_element_index=column.columnDescription.fFirstElementIndex,
                    min_value=column.columnDescription.fMinValue,
                    max_value=column.columnDescription.fMaxValue,
                )
                m_column.representations.append(m_representation)
                representations_by_physical_id[column.columnID] = m_representation

            for alias in schema.aliasColumnDescriptions:
                m_field = fields_by_source_id[alias.fFieldID]
                m_column = representations_by_physical_id[
                    alias.fPhysicalColumnID
                ].column

                if any(link.column is m_column for link in m_field.alias_columns):
                    continue

                m_field.alias_columns.append(m.AliasColumn(column=m_column))

            m_rntuple_instance = m.RNTupleInstance(rntuple=m_rntuple, file=m_input_file)
            session.add(m_rntuple_instance)

            n_entries = 0
            for group in footer_envelope.clusterGroups:
                m_cluster_group = m.ClusterGroup()
                m_rntuple_instance.cluster_groups.append(m_cluster_group)

                for _ in range(group.fNClusters):
                    cluster = next(clusters)
                    n_entries += cluster.summary.fNEntries

                    m_cluster = m.Cluster(
                        entry_start=cluster.summary.fFirstEntryNumber,
                        entry_stop=cluster.summary.fFirstEntryNumber
                        + cluster.summary.fNEntries,
                    )
                    m_cluster_group.clusters.append(m_cluster)

                    page_groups_by_physical_id: dict[int, m.PageGroup] = {}
                    ranges_with_pages = [r for r in cluster.columnRanges if r.pages]
                    for column_range in ranges_with_pages:
                        locations = column_range.pageLocations
                        assert locations is not None

                        settings = locations.compressionsettings
                        physical_id = column_range.column.columnID
                        m_page_group = m.PageGroup(
                            n_elements=column_range.nElements,
                            n_zero_elements=column_range.nZeroElements,
                            compression_settings=str(settings.compressionsettings)
                            if settings is not None
                            else None,
                            column_representation=representations_by_physical_id[
                                physical_id
                            ],
                        )
                        m_cluster.page_groups.append(m_page_group)
                        page_groups_by_physical_id[physical_id] = m_page_group

                    for bundle_pages in bundler(ranges_with_pages):
                        m_page_bundle: m.PageBundle | None = None
                        bundle_bytes = bytearray()

                        for column_range, page in bundle_pages:
                            m_page_group = page_groups_by_physical_id[
                                column_range.column.columnID
                            ]
                            source_range = reader.page_range(page)

                            if source_range not in placements:
                                if m_page_bundle is None:
                                    m_page_bundle = m.PageBundle(
                                        input_file=m_input_file,
                                        state=m.BundleState.PENDING_UPLOAD,
                                    )
                                    session.add(m_page_bundle)

                                bundle_offset = len(bundle_bytes)
                                bundle_bytes.extend(reader.page_bytes(f, page))
                                placements[source_range] = (
                                    m_page_bundle,
                                    bundle_offset,
                                )

                            page_bundle, bundle_offset = placements[source_range]
                            m_page = m.Page(
                                page_bundle=page_bundle,
                                page_group=m_page_group,
                                bundle_offset=bundle_offset,
                                first_element=page.firstElementInCluster,
                            )
                            session.add(m_page)

                        if m_page_bundle is not None:
                            session.flush()
                            object_path = object_directory / str(m_page_bundle.id)
                            _write_object(object_path, bundle_bytes, cleanup)
                            m_page_bundle.state = m.BundleState.AVAILABLE
            m_rntuple_instance.entries = n_entries

        extents = []
        for (file_offset, length), (page_bundle, bundle_offset) in sorted(
            placements.items()
        ):
            extents.append(
                {
                    "file_offset": file_offset,
                    "length": length,
                    "bundle_id": page_bundle.id,
                    "bundle_offset": bundle_offset,
                }
            )

        file_size = Path(file.file_path).stat().st_size
        non_page_bytes = bytearray()
        previous_page_end = 0
        for extent in extents:
            page_start = extent["file_offset"]
            page_end = page_start + extent["length"]

            if not previous_page_end <= page_start <= page_end <= file_size:
                msg = f"{file.file_path}: overlapping or out-of-bounds page at {page_start}"
                raise ValueError(msg)

            if previous_page_end < page_start:
                gap_size = page_start - previous_page_end
                non_page_bytes.extend(reader.read_bytes(f, previous_page_end, gap_size))
            previous_page_end = page_end

        if previous_page_end < file_size:
            non_page_bytes.extend(
                reader.read_bytes(f, previous_page_end, file_size - previous_page_end)
            )

        record_bytes = pack_record(file.file_path, file_size, extents, non_page_bytes)
        m_rehydration_info = m.RehydrationInfo(format_version=FORMAT_VERSION)
        m_input_file.rehydration_info = m_rehydration_info
        session.flush()
        record_path = (
            object_directory / f"rehydration-{m_rehydration_info.record_object_id}"
        )
        _write_object(record_path, record_bytes, cleanup)

        session.commit()
        cleanup.pop_all()
