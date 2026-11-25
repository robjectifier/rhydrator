"""Ingestion module for RHydrator."""

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime
import hashlib
import os
import pickle
from time import perf_counter
from rootfilespec import reader
from rootfilespec.bootstrap import ROOT3a3aRNTuple, TDatime
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import model as m
from ._terminal import Watershed
from .bundle import Bundler, one_page_per_object
from .record import FORMAT_VERSION, pack_record

# abbott: should use Rucio to get the metadata for the file
#   for now, just use a dictionary; a redundant class method but fine
#    can hardcode some dictionary values for test files for now
# maybe the wrapper passes the info/dictionary? or can i access Rucio from here?
@dataclass
class NewFile:
    """Data class representing a new file to be ingested."""
    dataset_name: str
    file_lfn: str
    file_path: str

def convert_TDatime(tdatime: TDatime) -> datetime | None:
    """convert root TDatime to regular python datetime object. 
    This function should become obsolete after resolving rootfilespec 'TODO: convert to datetime through a MemberSerDe' """

    from rootfilespec.bootstrap.TDatime import TDatime_to_datetime
    return TDatime_to_datetime(tdatime)

# function should take a dataset_name and return the dataset object if it exists, or create a new one if it doesn't
def get_dataset(session: Session, dataset_name: str, dataset_created_at: datetime | None, debug: bool = False) -> m.Dataset:
    """Retrieve or create a dataset in the database.

    Args:
        session (Session): SQLAlchemy session for database operations.
        dataset_name (str): Name of the dataset to retrieve or create.
        debug (bool): Print whether the dataset was created or reused.

    Returns:
        m.Dataset: The retrieved or newly created dataset object.
    """

    if debug:
        term = Watershed()
    dataset = session.scalars(
        select(m.Dataset).where(m.Dataset.name == dataset_name)
    ).one_or_none()
    if dataset is None:
        dataset = m.Dataset(name=dataset_name, created_at=dataset_created_at) # abbott: should this be time when file dir created or last time file dir was modified?
        session.add(dataset)
        session.flush()  # Ensure the dataset is added to the session and has an ID 
        # abbott: check w nick about flush
        if debug:
            print(f"\t\t\t{term.success}Created dataset{term.reset} {dataset_name!r} (id={dataset.id})")  # noqa: T201
    elif debug:
        print(f"\t\t\t{term.success}Reusing dataset{term.reset} {dataset_name!r} (id={dataset.id})")  # noqa: T201
    return dataset


def _write_object(path: str, data: bytes | bytearray, cleanup: ExitStack) -> None:
    """Remove this ingest's new object if writing or committing the file fails."""
    with open(path, "xb") as object_file:
        cleanup.callback(os.unlink, path)
        object_file.write(data)


def ingest_file(
    session: Session,
    file: NewFile,
    *,
    objects_dir: str | os.PathLike[str],
    bundler: Bundler = one_page_per_object,
    debug: bool = False,
) -> None:
    """Ingest a new file into the database.

    Args:
        session (Session): SQLAlchemy session for database operations.
        file (NewFile): Data class representing the new file to be ingested.
        objects_dir: Local directory for page bundles and the file's rehydration record.
        bundler: Group one cluster's page references into destination objects.
        debug (bool): Print progress while reading the file and writing its metadata.
    """

    if debug:
        term = Watershed()
        started = perf_counter()
        bundles_written = 0
        print(f"{term.title}Opening ROOT file {file.file_path}{term.reset}")  # noqa: T201
    with reader.open_path(file.file_path) as f, ExitStack() as cleanup:
        cleanup.callback(session.rollback)

        # ---- Step 1. Register the input file ----
        if debug:
            print(f"\t{term.step}Step 1:{term.reset} Register the input file {file.file_lfn!r}", flush=True)  # noqa: T201
        tdir = f.tfile.rootdir # root TDirectory for this ROOT file

        # 1.1 Find or create the Dataset
        if debug:
            print(f"\t\t{term.substep}Step 1.1:{term.reset} Find or create the Dataset", flush=True)  # noqa: T201
        m_dataset = get_dataset(session, file.dataset_name, convert_TDatime(tdir.header.fDatimeC), debug=debug)

        # 1.2 Create the InputFile and initialize its page placements
        if debug:
            print(f"\t\t{term.substep}Step 1.2:{term.reset} Create the InputFile and initialize its page placements", flush=True)  # noqa: T201
        _UUID = tdir.fUUID.fUUID if tdir.fUUID is not None else None # grabbing from here instead of ROOTFile header bc v302 header lacks UUID
        m_input_file = m.InputFile(
            lfn=file.file_lfn,
            uuid=_UUID,
        )
        m_dataset.input_files.append(m_input_file)  # Associate the InputFile with the Dataset

        placements: dict[tuple[int, int], tuple[m.PageBundle, int]] = {} # Once per input file. Shared pages keep their original bundle and offset.
        for tkey in f.keylist().fKeys: # Iterate over the TKeys in file
            # ---- Step 2. Read the RNTuple schema and register its instance ----
            if debug:
                print(f"\t{term.step}Step 2:{term.reset} Read the RNTuple schema and register its instance", flush=True)  # noqa: T201

            # 2.1 Read the anchor and schema, then create the RNTuple
            if debug:
                print(f"\t\t{term.substep}Step 2.1:{term.reset} Read the anchor and schema, then create the RNTuple", flush=True)  # noqa: T201
            anchor = f.fetch(tkey)
            if not isinstance(anchor, ROOT3a3aRNTuple): # Ensure each TKey corresponds to an RNTuple
                msg = f"Expected an RNTuple anchor, got {type(anchor).__name__}"
                raise TypeError(msg)

            source_rntuple = f.fetch.rntuple(anchor)
            header_envelope = source_rntuple.headerEnvelope
            footer_envelope = source_rntuple.footerEnvelope
            pagelist_envelopes = source_rntuple.pagelistEnvelopes
            schema = source_rntuple.schemaDescription
            if debug:
                print(f"\t\t\tRNTuple: {header_envelope.fName!r}", flush=True)  # noqa: T201
            schema_hash = hashlib.md5(pickle.dumps(schema, protocol=5)).hexdigest()

            m_rntuple = m.RNTuple(
                name = header_envelope.fName,
                description = header_envelope.fDescription,
                schema_hash = schema_hash,
            )
            m_dataset.rntuples.append(m_rntuple)  # Associate the RNTuple with the Dataset

            # 2.2 Create the fields, then link children to their parents
            if debug:
                print(f"\t\t{term.substep}Step 2.2:{term.reset} Create the fields, then link children to their parents", flush=True)  # noqa: T201
            fields_by_source_id: list[m.Field] = [] # Track fields by their source ID for later association with parents
            for field in schema.fieldDescriptions:
                m_field = m.Field(
                    # rntuple_id = m_rntuple.id,
                    version = field.fFieldVersion,
                    type_version = field.fTypeVersion,
                    name = field.fFieldName,
                    type_name = field.fTypeName,
                    type_alias = field.fTypeAlias,
                    description = field.fFieldDescription,
                    array_size = field.fArraySize,
                    type_checksum = field.fTypeChecksum
                )
                m_rntuple.fields.append(m_field)  # Associate the Field with the RNTuple
                fields_by_source_id.append(m_field)

            # Now that all fields are created, associate parent-child relationships
            for _field_id, _field in enumerate(schema.fieldDescriptions):
                parent_field_id = _field.fParentFieldID
                if parent_field_id != _field_id: # If the field has a parent (i.e., it's not a top-level field)
                    child = fields_by_source_id[_field_id]
                    child.parent_id = fields_by_source_id[parent_field_id].id # Set the parent ID for the child field
                    child.parent = fields_by_source_id[parent_field_id] # Associate the child field with its parent

            # 2.3 Create columns and representations, then link aliases
            if debug:
                print(f"\t\t{term.substep}Step 2.3:{term.reset} Create columns and representations, then link aliases", flush=True)  # noqa: T201
            columns_by_field_and_index: dict[tuple[int, int], m.Column] = {} # Track columns by (field ID, index) for later association with representations
            representations_by_physical_id: dict[int, m.ColumnRepresentation] = {} # Track representations by their physical column ID for later association with clusters
            for column in source_rntuple.columns(): # This iterates over all columns (including each representation of a column) in the RNTuple
                key = (column.columnDescription.fFieldID, column.index) # Use the field ID and index to uniquely identify each column

                # Reuse the logical column across its representations.
                m_column = columns_by_field_and_index.get(key) # Check if the column already exists (i.e., if we've already created a column for this field ID and index)
                if m_column is None: # If the column doesn't exist yet, create it and associate it with the corresponding field
                    m_column = m.Column()
                    m_field = fields_by_source_id[column.columnDescription.fFieldID]
                    m_field.columns.append(m_column)
                    columns_by_field_and_index[key] = m_column # Store the newly created column in the dictionary for future reference
                
                # Look up each representation later by its physical column ID.
                m_representation = m.ColumnRepresentation(
                    column_type=column.columnDescription.fColumnType.value,
                    bits_on_storage=column.columnDescription.fBitsOnStorage,
                    first_element_index=column.columnDescription.fFirstElementIndex,
                    min_value=column.columnDescription.fMinValue,
                    max_value=column.columnDescription.fMaxValue,
                )
                m_column.representations.append(m_representation) # Associate the ColumnRepresentation with the Column
                representations_by_physical_id[column.columnID] = m_representation # Track the representation by its physical column ID for later association with clusters

            # Link each alias field to its logical column.
            for alias in schema.aliasColumnDescriptions:
                m_field = fields_by_source_id[alias.fFieldID]
                m_column = representations_by_physical_id[alias.fPhysicalColumnID].column

                # Check if the alias column already exists for this field and column combination
                if any(link.column is m_column for link in m_field.alias_columns):
                    continue

                m_field.alias_columns.append(m.AliasColumn(column=m_column)) # Create a new AliasColumn entry linking the field & column to it

            # 2.4 Create the RNTupleInstance for this input file
            if debug:
                print(f"\t\t{term.substep}Step 2.4:{term.reset} Create the RNTupleInstance for this input file", flush=True)  # noqa: T201
            m_rntuple_instance = m.RNTupleInstance(rntuple=m_rntuple, file=m_input_file) # Associate the RNTupleInstance with the RNTuple and InputFile
            session.add(m_rntuple_instance)  # Tell SQLAlchemy to track this RNTupleInstance and insert its row the next time it flushes
            if debug:
                total_clusters = sum(group.fNClusters for group in footer_envelope.clusterGroups)
                print(  # noqa: T201
                    f"\t\t\t{term.success}Schema ready:{term.reset} {len(fields_by_source_id):,} fields, "
                    f"{len(columns_by_field_and_index):,} logical columns, "
                    f"{len(representations_by_physical_id):,} physical representations. "
                    f"{total_clusters:,} clusters to process.",
                    flush=True,
                )

            # ---- Step 3. Store this RNTuple's clusters and pages ----
            if debug:
                print(f"\t{term.step}Step 3:{term.reset} Store this RNTuple's clusters and pages", flush=True)  # noqa: T201
            clusters = source_rntuple.clusters()
            nEntries = 0 # Track number of entries across all clusters for this RNTupleInstance
            # 3.1 Create each ClusterGroup and its Clusters
            if debug:
                print(f"\t\t{term.substep}Step 3.1:{term.reset} Create each ClusterGroup and its Clusters", flush=True)  # noqa: T201
            for group in footer_envelope.clusterGroups:
                m_cluster_group = m.ClusterGroup()
                m_rntuple_instance.cluster_groups.append(m_cluster_group)  # Associate the ClusterGroup with the RNTupleInstance

                for _ in range(group.fNClusters): # Iterate over the number of clusters in the cluster group
                    cluster = next(clusters) # Get the next cluster from the generator
                    nEntries += cluster.summary.fNEntries # Update the total number of entries for this RNTupleInstance

                    m_cluster = m.Cluster(
                        entry_start = cluster.summary.fFirstEntryNumber,
                        entry_stop = cluster.summary.fFirstEntryNumber + cluster.summary.fNEntries,
                    )
                    m_cluster_group.clusters.append(m_cluster)  # Associate the Cluster with the ClusterGroup

                    # 3.2 Create one PageGroup per column range with stored pages
                    if debug:
                        print(f"\t\t{term.substep}Step 3.2:{term.reset} Create one PageGroup per column range with stored pages", flush=True)  # noqa: T201
                    page_groups_by_physical_id: dict[int, m.PageGroup] = {} # Track page groups by their physical column ID for later association with pages
                    ranges_with_pages = [r for r in cluster.columnRanges if r.pages] # Only ranges with stored pages need a page group
                    if debug:
                        print(  # noqa: T201
                            f"\t\t\t{term.mist}{term.level(cluster.clusterID + 1, total_clusters)} Cluster: {cluster.clusterID + 1:,}/{total_clusters:,}, "
                            f"{cluster.summary.fNEntries:,} entries, {len(ranges_with_pages):,} columns with pages{term.reset}",
                            flush=True,
                        )
                    for column_range in ranges_with_pages:
                        locations = column_range.pageLocations
                        assert locations is not None  # A range with stored pages has a page list.

                        settings = locations.compressionsettings
                        physical_id = column_range.column.columnID
                        m_page_group = m.PageGroup(
                            n_elements=column_range.nElements,
                            n_zero_elements=column_range.nZeroElements,
                            compression_settings=str(settings.compressionsettings) if settings is not None else None,
                            column_representation=representations_by_physical_id[physical_id],
                        )
                        m_cluster.page_groups.append(m_page_group)
                        page_groups_by_physical_id[physical_id] = m_page_group
                    # End ranges_with_pages loop

                    # 3.3 Bundle this cluster's pages
                    if debug:
                        print(f"\t\t{term.substep}Step 3.3:{term.reset} Bundle this cluster's pages", flush=True)  # noqa: T201
                    for bundle_pages in bundler(ranges_with_pages): # Group pages into bundles based on the bundler function
                        m_page_bundle: m.PageBundle | None = None # Init to None, but will be set to a new PageBundle 
                        bundle_bytes = bytearray() # Initialize a bytearray to hold the concatenated bytes for this bundle

                        for column_range, page in bundle_pages: # Iterate over the pages in the bundle
                            m_page_group = page_groups_by_physical_id[column_range.column.columnID]
                            locator = page.pageDescription.page_locator
                            source_range = (locator.offset, locator.size) # locator.offset and locator.size identify the bytes in the input file

                            if source_range not in placements: # If this page hasn't been placed in a bundle yet, fetch its bytes and add them to the current bundle
                                if m_page_bundle is None:  # The first new page in this bundle needs a destination object
                                    m_page_bundle = m.PageBundle(
                                        input_file=m_input_file, # Associate the PageBundle with the InputFile
                                        state=m.BundleState.PENDING_UPLOAD
                                    )
                                    session.add(m_page_bundle)  # Tell SQLAlchemy to track this PageBundle and insert its row the next time it flushes

                                # abbott: ask nick if this is hacky or fine. i.e. does this call for an update to rootfilespec to allow for a 
                                #   more direct "read" method that takes a locator and returns the bytes for that page?
                                #  like we are using InterpretablePage, but maybe it needs a method for giving raw bytes
                                raw = f.fetch.buffer(locator) # Fetch the raw bytes for this page from the input file
                                locator.read_from(raw) # Have rootfilespec verify the checksum (if present)

                                bundle_offset = len(bundle_bytes) # The offset in the bundle where this page's bytes will start
                                bundle_bytes.extend(raw.data) # Append the raw bytes for this page to the bundle
                                placements[source_range] = (m_page_bundle, bundle_offset) # Record the placement of this page in the bundle

                            # Create a Page row for every reference, including shared pages.
                            page_bundle, bundle_offset = placements[source_range] # Get the bundle and offset for this page
                            m_page = m.Page(
                                page_bundle=page_bundle,
                                page_group=m_page_group,
                                bundle_offset=bundle_offset,
                                first_element=page.firstElementInCluster
                            )
                            session.add(m_page)  # Tell SQLAlchemy to track this Page and insert its row the next time it flushes
                        # End bundle_pages loop

                        # Write the completed bundle and mark it available.
                        if m_page_bundle is not None:
                            session.flush()  # Get the database-generated bundle ID.
                            object_path = os.path.join(objects_dir, str(m_page_bundle.id))
                            _write_object(object_path, bundle_bytes, cleanup)
                            m_page_bundle.state = m.BundleState.AVAILABLE
                            if debug:
                                bundles_written += 1
                                if bundles_written % 1_000 == 0:
                                    print(  # noqa: T201
                                        f"\t\t\t{term.mist}{term.dot} File so far: {bundles_written:,} page bundles written, "
                                        f"{len(placements):,} distinct page ranges placed "
                                        f"({perf_counter() - started:.1f} s elapsed){term.reset}",
                                        flush=True,
                                    )
                    # End bundler loop
                # End cluster loop
            # End clusterGroup loop
            # 3.4 Set the instance's entry count after all its clusters
            if debug:
                print(f"\t\t{term.substep}Step 3.4:{term.reset} Set the instance's entry count after all its clusters", flush=True)  # noqa: T201
            m_rntuple_instance.entries = nEntries # Update the total number of entries for this RNTupleInstance
        # End TKey loop

        # ---- Step 4. Finish the input file ----
        if debug:
            print(f"\t{term.step}Step 4:{term.reset} Finish the input file", flush=True)  # noqa: T201

        # 4.1 Build extents from placements in source-file order
        if debug:
            print(f"\t\t{term.substep}Step 4.1:{term.reset} Build extents from placements in source-file order", flush=True)  # noqa: T201
        extents = [] # Holds metadata for all byte ranges that ARENT included in the rehydration blob
        for (file_offset, length), (page_bundle, bundle_offset) in sorted(placements.items()):
            # Store the metadata for each page's byte range in the extents list, in file order
            extents.append({
                "file_offset": file_offset,
                "length": length,
                "bundle_id": page_bundle.id,
                "bundle_offset": bundle_offset,
            })

        # 4.2 Collect the non-page bytes before, between, and after the extents
        if debug:
            print(  # noqa: T201
                f"\t\t{term.substep}Step 4.2:{term.reset} Collect the non-page bytes before, between, and after the extents"
                f"\n\t\t\tExtents: {len(extents):,} after writing {bundles_written:,} page bundles",
                flush=True,
            )
        file_size = os.path.getsize(file.file_path)
        non_page_bytes = bytearray()
        previous_page_end = 0
        for extent in extents:
            page_start = extent["file_offset"]
            page_end = page_start + extent["length"]  # Exclusive

            if not previous_page_end <= page_start <= page_end <= file_size:
                msg = f"{file.file_path}: overlapping or out-of-bounds page at {page_start}"
                raise ValueError(msg)

            if previous_page_end < page_start: # Copy the gap before this page
                gap_size = page_start - previous_page_end
                non_page_bytes.extend(f.fetch.buffer_at(previous_page_end, gap_size).data)
            previous_page_end = page_end

        if previous_page_end < file_size: # Copy everything after the final page
            non_page_bytes.extend(f.fetch.buffer_at(previous_page_end, file_size - previous_page_end).data)
        # abbott: once again messing around with the buffer; ask nick if too hacky or we should add smth to rootfilespec

        # 4.3 Pack the rehydration record, reserve its object ID, and write it
        if debug:
            print(  # noqa: T201
                f"\t\t{term.substep}Step 4.3:{term.reset} Pack the rehydration record, reserve its object ID, and write it"
                f"\n\t\t\tChecksumming {file_size:,} input bytes and packing "
                f"{len(non_page_bytes):,} non-page bytes into the record",
                flush=True,
            )
        record_bytes = pack_record(file.file_path, file_size, extents, non_page_bytes)
        m_rehydration_info = m.RehydrationInfo(format_version=FORMAT_VERSION)
        m_input_file.rehydration_info = m_rehydration_info
        session.flush()  # Reserve the record object ID before writing its bytes.
        record_path = os.path.join(objects_dir, f"rehydration-{m_rehydration_info.record_object_id}")
        _write_object(record_path, record_bytes, cleanup)

        if debug:
            print(  # noqa: T201
                f"\t\t\t{term.success}Wrote{term.reset} {record_path} ({len(record_bytes):,} bytes).",
                flush=True,
            )
        # Final Step: Commit the file's metadata
        if debug:
            print(f"\t{term.step}Final Step:{term.reset} Commit the file's metadata", flush=True)  # noqa: T201
        session.commit()
        cleanup.pop_all()  # The committed database now references these objects.
        if debug:
            print(f"\t\t{term.success}Committed session for {file.file_lfn!r}{term.reset}\n")  # noqa: T201

