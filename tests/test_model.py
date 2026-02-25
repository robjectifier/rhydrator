"""Test script to create tables and insert dummy data."""

import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from rhydrator.model import (
    AliasColumn,
    Base,
    Cluster,
    ClusterGroup,
    Column,
    ColumnRepresentation,
    Dataset,
    Field,
    InputFile,
    Object,
    Page,
    PageGroup,
    RehydrationInfo,
    RNTuple,
    RNTupleInstance,
)


@pytest.fixture
def db_session():
    """Create a temporary SQLite database with all tables."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        yield session

    # Cleanup
    engine.dispose()
    db_path.unlink(missing_ok=True)


def test_create_tables_and_insert_data(db_session):
    """Test creating all tables and inserting dummy data."""
    session = db_session

    # Create a dataset
    dataset = Dataset(id=1, created_at=datetime.now())
    session.add(dataset)

    # Create input files
    input_file1 = InputFile(
        id=1,
        dataset_id=1,
        uuid=uuid4(),
        lfn="/path/to/file1.root",
        entries=1000,
    )
    input_file2 = InputFile(
        id=2,
        dataset_id=1,
        uuid=uuid4(),
        lfn="/path/to/file2.root",
        entries=2000,
    )
    session.add_all([input_file1, input_file2])

    # Create an RNTuple
    rntuple = RNTuple(
        id=1,
        dataset_id=1,
        name="Events",
        description="Event data ntuple",
    )
    session.add(rntuple)

    # Create RNTuple instances (one per file)
    instance1 = RNTupleInstance(id=1, file_id=1, rntuple_id=1)
    instance2 = RNTupleInstance(id=2, file_id=2, rntuple_id=1)
    session.add_all([instance1, instance2])

    # Create fields (with parent-child hierarchy)
    parent_field = Field(
        id=1,
        rntuple_id=1,
        parent_id=None,
        version=1,
        type_version=1,
        name="event",
        type_name="Event",
        type_alias="Event",
        description="Top-level event field",
    )
    child_field1 = Field(
        id=2,
        rntuple_id=1,
        parent_id=1,
        version=1,
        type_version=1,
        name="pt",
        type_name="float",
        type_alias="float",
        description="Transverse momentum",
        array_size=None,
        type_checksum=12345,
    )
    child_field2 = Field(
        id=3,
        rntuple_id=1,
        parent_id=1,
        version=1,
        type_version=1,
        name="eta",
        type_name="float",
        type_alias="float",
        description="Pseudorapidity",
    )
    session.add_all([parent_field, child_field1, child_field2])

    # Create columns
    column1 = Column(id=1, field_id=2)
    column2 = Column(id=2, field_id=3)
    session.add_all([column1, column2])

    # Create column representations
    col_rep1 = ColumnRepresentation(
        id=1,
        column_id=1,
        column_type=1,
        bits_on_storage=32,
        first_element_index=0,
        min_value=-100.0,
        max_value=1000.0,
    )
    col_rep2 = ColumnRepresentation(
        id=2,
        column_id=2,
        column_type=1,
        bits_on_storage=32,
        first_element_index=0,
        min_value=-5.0,
        max_value=5.0,
    )
    session.add_all([col_rep1, col_rep2])

    # Create cluster groups and clusters
    cluster_group = ClusterGroup(id=1, rntuple_instance_id=1)
    session.add(cluster_group)

    cluster1 = Cluster(id=1, cluster_group_id=1, entry_start=0, entry_stop=500)
    cluster2 = Cluster(id=2, cluster_group_id=1, entry_start=500, entry_stop=1000)
    session.add_all([cluster1, cluster2])

    # Create objects
    obj1 = Object(id=1, input_file_id=1)
    obj2 = Object(id=2, input_file_id=1)
    session.add_all([obj1, obj2])

    # Create page groups
    page_group1 = PageGroup(
        id=1,
        object_id=1,
        cluster_id=1,
        column_rep_id=1,
        element_offset=0,
        compression_settings="zstd:5",
    )
    page_group2 = PageGroup(
        id=2,
        object_id=2,
        cluster_id=2,
        column_rep_id=2,
        element_offset=0,
        compression_settings="lz4:1",
    )
    session.add_all([page_group1, page_group2])

    # Create pages
    page1 = Page(id=1, page_group_id=1, index=0)
    page2 = Page(id=2, page_group_id=1, index=1)
    page3 = Page(id=3, page_group_id=2, index=0)
    session.add_all([page1, page2, page3])

    # Create alias column
    alias = AliasColumn(field_id=2, column_id=1)
    session.add(alias)

    # Create rehydration info
    rehydration = RehydrationInfo(id=1, input_file_id=1)
    session.add(rehydration)

    # Commit all data
    session.commit()

    # Verify data was inserted by querying
    assert session.query(Dataset).count() == 1
    assert session.query(InputFile).count() == 2
    assert session.query(RNTuple).count() == 1
    assert session.query(RNTupleInstance).count() == 2
    assert session.query(Field).count() == 3
    assert session.query(Column).count() == 2
    assert session.query(ColumnRepresentation).count() == 2
    assert session.query(ClusterGroup).count() == 1
    assert session.query(Cluster).count() == 2
    assert session.query(Object).count() == 2
    assert session.query(PageGroup).count() == 2
    assert session.query(Page).count() == 3
    assert session.query(AliasColumn).count() == 1
    assert session.query(RehydrationInfo).count() == 1

    # Test relationships
    dataset = session.query(Dataset).first()
    assert len(dataset.input_files) == 2
    assert len(dataset.rntuples) == 1

    rntuple = session.query(RNTuple).first()
    assert len(rntuple.fields) == 3
    assert len(rntuple.instances) == 2

    parent = session.query(Field).filter_by(name="event").first()
    assert len(parent.children) == 2
    assert parent.parent is None

    child = session.query(Field).filter_by(name="pt").first()
    assert child.parent.name == "event"

    print("\n✅ All tables created and dummy data inserted successfully!")
