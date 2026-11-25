"""SQLAlchemy ORM models for rhydrator."""

from datetime import datetime
import enum
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---- Dataset group ----

class Dataset(Base):
    """Table `dataset`.

    ```python
    # Columns
    id:         int
    name:       str
    created_at: datetime | None

    # Relationships
    input_files: list[InputFile]
    rntuples:    list[RNTuple]
    ```
    """

    __tablename__ = "dataset"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    """Dataset name."""
    created_at: Mapped[datetime | None]
    """Date the dataset was created.""" # abbott: should this be time when file dir created or last time file dir was modified?

    # Relationships
    input_files: Mapped[list["InputFile"]] = relationship(back_populates="dataset")
    rntuples: Mapped[list["RNTuple"]] = relationship(back_populates="dataset")


class InputFile(Base):
    """Table `input_file`.

    ```python
    # Columns
    id:         int
    dataset_id: int
    uuid:       UUID | None
    lfn:        str | None

    # Relationships
    dataset:           Dataset
    rntuple_instances: list[RNTupleInstance]
    page_bundles:      list[PageBundle]
    rehydration_info:  RehydrationInfo
    ```
    """

    __tablename__ = "input_file"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("dataset.id"), nullable=False)
    uuid: Mapped[UUID | None]
    """File UUID."""
    lfn: Mapped[str | None] = mapped_column(String)
    """File logical name."""

    # Relationships
    dataset: Mapped[Dataset] = relationship(back_populates="input_files")
    page_bundles: Mapped[list["PageBundle"]] = relationship(back_populates="input_file")
    rehydration_info: Mapped["RehydrationInfo"] = relationship(
        back_populates="input_file"
    )
    rntuple_instances: Mapped[list["RNTupleInstance"]] = relationship(
        back_populates="file"
    )


# ---- ObjectStore group ----

class BundleState(enum.Enum):
    PENDING_UPLOAD = "pending_upload"
    AVAILABLE = "available"
    OFFLOADED_TO_TAPE = "offloaded_to_tape"
    DEPRECATED = "deprecated"
    DELETED = "deleted"

class PageBundle(Base):
    """Table `page_bundle`.

    ```python
    # Columns
    id:            int
    input_file_id: int
    state:         BundleState

    # Relationships
    input_file: InputFile
    pages:      list[Page]
    ```
    """

    __tablename__ = "page_bundle"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    input_file_id: Mapped[int] = mapped_column(
        ForeignKey("input_file.id"), nullable=False
    )
    state: Mapped[BundleState]
    """Last known upload state of the page bundle (i.e. the Ceph S3 object)"""

    # Relationships
    input_file: Mapped[InputFile] = relationship(back_populates="page_bundles")
    pages: Mapped[list["Page"]] = relationship(back_populates="page_bundle")


class RehydrationInfo(Base):
    """One MessagePack record per input file, stored at objects/rehydration-<record_object_id>."""

    __tablename__ = "rehydration_info"

    record_object_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    input_file_id: Mapped[int] = mapped_column(
        ForeignKey("input_file.id"), nullable=False, unique=True
    )
    format_version: Mapped[int] = mapped_column(Integer, nullable=False)

    # Relationships
    input_file: Mapped[InputFile] = relationship(back_populates="rehydration_info")


# ---- RNTuple group ----


class RNTuple(Base):
    """Table `rntuple`.

    ```python
    # Columns
    id:          int
    dataset_id:  int
    name:        str | None
    description: str | None
    schema_hash: str

    # Relationships
    dataset:   Dataset
    instances: list[RNTupleInstance]
    fields:    list[Field]
    ```
    """

    __tablename__ = "rntuple"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("dataset.id"), nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(String)
    schema_hash: Mapped[str] = mapped_column(String)

    # Relationships
    dataset: Mapped[Dataset] = relationship(back_populates="rntuples")
    instances: Mapped[list["RNTupleInstance"]] = relationship(back_populates="rntuple")
    fields: Mapped[list["Field"]] = relationship(back_populates="rntuple")


class RNTupleInstance(Base):
    """Table `rntuple_instance`.

    ```python
    # Columns
    id:         int
    file_id:    int
    rntuple_id: int
    entries:    int | None

    # Relationships
    file:           InputFile
    rntuple:        RNTuple
    cluster_groups: list[ClusterGroup]
    ```
    """

    __tablename__ = "rntuple_instance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("input_file.id"), nullable=False)
    rntuple_id: Mapped[int] = mapped_column(ForeignKey("rntuple.id"), nullable=False)
    entries: Mapped[int | None] = mapped_column(Integer)

    # Relationships
    file: Mapped[InputFile] = relationship(back_populates="rntuple_instances")
    rntuple: Mapped[RNTuple] = relationship(back_populates="instances")
    cluster_groups: Mapped[list["ClusterGroup"]] = relationship(
        back_populates="rntuple_instance"
    )


class ClusterGroup(Base):
    """Table `cluster_group`.

    ```python
    # Columns
    id:                  int
    rntuple_instance_id: int

    # Relationships
    rntuple_instance: RNTupleInstance
    clusters:         list[Cluster]
    ```
    """

    __tablename__ = "cluster_group"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rntuple_instance_id: Mapped[int] = mapped_column(
        ForeignKey("rntuple_instance.id"), nullable=False
    )

    # Relationships
    rntuple_instance: Mapped[RNTupleInstance] = relationship(
        back_populates="cluster_groups"
    )
    clusters: Mapped[list["Cluster"]] = relationship(back_populates="cluster_group")


class Cluster(Base):
    """Table `cluster`.

    ```python
    # Columns
    id:               int
    cluster_group_id: int
    entry_start:      int | None
    entry_stop:       int | None

    # Relationships
    cluster_group: ClusterGroup
    page_groups:   list[PageGroup]
    ```
    """

    __tablename__ = "cluster"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_group_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_group.id"), nullable=False
    )
    entry_start: Mapped[int | None] = mapped_column(Integer)
    entry_stop: Mapped[int | None] = mapped_column(Integer)

    # Relationships
    cluster_group: Mapped[ClusterGroup] = relationship(back_populates="clusters")
    page_groups: Mapped[list["PageGroup"]] = relationship(back_populates="cluster")


class Field(Base):
    """Table `field`.

    ```python
    # Columns
    id:            int
    rntuple_id:    int
    parent_id:     int | None
    version:       int
    type_version:  int
    name:          str
    type_name:     str
    type_alias:    str
    description:   str
    array_size:    int | None
    type_checksum: int | None

    # Relationships
    rntuple:       RNTuple
    parent:        Field | None
    children:      list[Field]
    columns:       list[Column]
    alias_columns: list[AliasColumn]
    ```
    """

    __tablename__ = "field"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rntuple_id: Mapped[int] = mapped_column(ForeignKey("rntuple.id"), nullable=False)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("field.id"))
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    type_version: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type_name: Mapped[str] = mapped_column(String, nullable=False)
    type_alias: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    array_size: Mapped[int | None] = mapped_column(Integer)
    type_checksum: Mapped[int | None] = mapped_column(Integer)

    # Relationships
    rntuple: Mapped[RNTuple] = relationship(back_populates="fields")
    parent: Mapped[Optional["Field"]] = relationship(
        back_populates="children", remote_side=[id]
    )
    children: Mapped[list["Field"]] = relationship(back_populates="parent")
    columns: Mapped[list["Column"]] = relationship(back_populates="field")
    alias_columns: Mapped[list["AliasColumn"]] = relationship(back_populates="field")


class Column(Base):
    """Table `column`.

    ```python
    # Columns
    id:       int
    field_id: int

    # Relationships
    field:           Field
    representations: list[ColumnRepresentation]
    alias_columns:   list[AliasColumn]
    ```
    """

    __tablename__ = "column"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id"), nullable=False)

    # Relationships
    field: Mapped[Field] = relationship(back_populates="columns")
    representations: Mapped[list["ColumnRepresentation"]] = relationship(
        back_populates="column"
    )
    alias_columns: Mapped[list["AliasColumn"]] = relationship(back_populates="column")


class ColumnRepresentation(Base):
    """Table `column_representation`.

    ```python
    # Columns
    id:                  int
    column_id:           int
    column_type:         int
    bits_on_storage:     int
    first_element_index: int | None
    min_value:           float | None
    max_value:           float | None

    # Relationships
    column:      Column
    page_groups: list[PageGroup]
    ```
    """

    __tablename__ = "column_representation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("column.id"), nullable=False)
    column_type: Mapped[int] = mapped_column(Integer, nullable=False)
    bits_on_storage: Mapped[int] = mapped_column(Integer, nullable=False)
    first_element_index: Mapped[int | None] = mapped_column(Integer)
    min_value: Mapped[float | None] = mapped_column(Float)
    max_value: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="representations")
    page_groups: Mapped[list["PageGroup"]] = relationship(
        back_populates="column_representation"
    )


class AliasColumn(Base):
    """Table `alias_column`.

    ```python
    # Columns
    field_id:  int
    column_id: int

    # Relationships
    field:  Field
    column: Column
    ```
    """

    __tablename__ = "alias_column"

    field_id: Mapped[int] = mapped_column(
        ForeignKey("field.id"), primary_key=True, nullable=False
    )
    column_id: Mapped[int] = mapped_column(
        ForeignKey("column.id"), primary_key=True, nullable=False
    )

    # Relationships
    field: Mapped[Field] = relationship(back_populates="alias_columns")
    column: Mapped[Column] = relationship(back_populates="alias_columns")

# sometimes we are ok with loading the relationship every time, 
# other times there are soooo many that we only want to load it when we need it
# e.g. for each column rep, there are very many page groups
# look into sqlalchemy for this (lazy loading)


class PageGroup(Base): # abbott: rename to column rep instance?
    """Table `page_group`.

    ```python
    # Columns
    id:                   int
    cluster_id:           int
    column_rep_id:        int
    n_elements:           int
    n_zero_elements:      int
    compression_settings: str | None

    # Relationships
    cluster:               Cluster
    column_representation: ColumnRepresentation
    pages:                 list[Page]
    ```
    """

    __tablename__ = "page_group"
    __table_args__ = (UniqueConstraint("cluster_id", "column_rep_id"),) # abbott: can make unique id? overkill? prolly not a good idea bc page would have to store two things instead of one

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(ForeignKey("cluster.id"), nullable=False)
    column_rep_id: Mapped[int] = mapped_column(
        ForeignKey("column_representation.id"), nullable=False
    )
    n_elements: Mapped[int] = mapped_column(BigInteger, nullable=False)
    """Total elements of this column in the cluster, including leading implicit zeros."""
    n_zero_elements: Mapped[int] = mapped_column(BigInteger, nullable=False)
    """Leading elements that read as zero and have no stored page."""
    compression_settings: Mapped[str | None] = mapped_column(String)
    """Compression settings used for the pages of this column in this cluster."""

    # Relationships
    cluster: Mapped[Cluster] = relationship(back_populates="page_groups")
    column_representation: Mapped[ColumnRepresentation] = relationship(
        back_populates="page_groups"
    )
    pages: Mapped[list["Page"]] = relationship(back_populates="page_group")


class Page(Base):
    """Table `page`.

    ```python
    # Columns
    id:            int
    page_group_id: int
    first_element: int | None
    bundle_id:     int
    bundle_offset: int

    # Relationships
    page_bundle: PageBundle
    page_group:  PageGroup
    ```
    """

    __tablename__ = "page"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    page_group_id: Mapped[int] = mapped_column(
        ForeignKey("page_group.id"), nullable=False
    )
    first_element: Mapped[int | None] = mapped_column(BigInteger)
    """Index of the page's first element within this column in this cluster."""
    bundle_id: Mapped[int] = mapped_column(ForeignKey("page_bundle.id"), nullable=False)
    bundle_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    """Offset of the page's data within the page bundle (Ceph S3 object)."""

    # Relationships
    page_bundle: Mapped[PageBundle] = relationship(back_populates="pages")
    page_group: Mapped[PageGroup] = relationship(back_populates="pages")
