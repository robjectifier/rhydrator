"""SQLAlchemy ORM models for rhydrator."""

import enum
from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_at: Mapped[datetime | None]

    input_files: Mapped[list["InputFile"]] = relationship(back_populates="dataset")
    rntuples: Mapped[list["RNTuple"]] = relationship(back_populates="dataset")


class InputFile(Base):
    __tablename__ = "input_file"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("dataset.id"), nullable=False)
    uuid: Mapped[UUID | None]
    lfn: Mapped[str | None] = mapped_column(String)

    dataset: Mapped[Dataset] = relationship(back_populates="input_files")
    page_bundles: Mapped[list["PageBundle"]] = relationship(back_populates="input_file")
    rehydration_info: Mapped["RehydrationInfo"] = relationship(
        back_populates="input_file"
    )
    rntuple_instances: Mapped[list["RNTupleInstance"]] = relationship(
        back_populates="file"
    )


class BundleState(enum.Enum):
    PENDING_UPLOAD = "pending_upload"
    AVAILABLE = "available"
    OFFLOADED_TO_TAPE = "offloaded_to_tape"
    DEPRECATED = "deprecated"
    DELETED = "deleted"


class PageBundle(Base):
    __tablename__ = "page_bundle"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    input_file_id: Mapped[int] = mapped_column(
        ForeignKey("input_file.id"), nullable=False
    )
    state: Mapped[BundleState]
    """Last known upload state of the page bundle (i.e. the Ceph S3 object)"""

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

    input_file: Mapped[InputFile] = relationship(back_populates="rehydration_info")


class RNTuple(Base):
    __tablename__ = "rntuple"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("dataset.id"), nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(String)
    schema_hash: Mapped[str] = mapped_column(String)

    dataset: Mapped[Dataset] = relationship(back_populates="rntuples")
    instances: Mapped[list["RNTupleInstance"]] = relationship(back_populates="rntuple")
    fields: Mapped[list["Field"]] = relationship(back_populates="rntuple")


class RNTupleInstance(Base):
    __tablename__ = "rntuple_instance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("input_file.id"), nullable=False)
    rntuple_id: Mapped[int] = mapped_column(ForeignKey("rntuple.id"), nullable=False)
    entries: Mapped[int | None] = mapped_column(Integer)

    file: Mapped[InputFile] = relationship(back_populates="rntuple_instances")
    rntuple: Mapped[RNTuple] = relationship(back_populates="instances")
    cluster_groups: Mapped[list["ClusterGroup"]] = relationship(
        back_populates="rntuple_instance"
    )


class ClusterGroup(Base):
    __tablename__ = "cluster_group"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rntuple_instance_id: Mapped[int] = mapped_column(
        ForeignKey("rntuple_instance.id"), nullable=False
    )

    rntuple_instance: Mapped[RNTupleInstance] = relationship(
        back_populates="cluster_groups"
    )
    clusters: Mapped[list["Cluster"]] = relationship(back_populates="cluster_group")


class Cluster(Base):
    __tablename__ = "cluster"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_group_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_group.id"), nullable=False
    )
    entry_start: Mapped[int | None] = mapped_column(Integer)
    entry_stop: Mapped[int | None] = mapped_column(Integer)

    cluster_group: Mapped[ClusterGroup] = relationship(back_populates="clusters")
    page_groups: Mapped[list["PageGroup"]] = relationship(back_populates="cluster")


class Field(Base):
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

    rntuple: Mapped[RNTuple] = relationship(back_populates="fields")
    parent: Mapped[Optional["Field"]] = relationship(
        back_populates="children", remote_side=[id]
    )
    children: Mapped[list["Field"]] = relationship(back_populates="parent")
    columns: Mapped[list["Column"]] = relationship(back_populates="field")
    alias_columns: Mapped[list["AliasColumn"]] = relationship(back_populates="field")


class Column(Base):
    __tablename__ = "column"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    field_id: Mapped[int] = mapped_column(ForeignKey("field.id"), nullable=False)

    field: Mapped[Field] = relationship(back_populates="columns")
    representations: Mapped[list["ColumnRepresentation"]] = relationship(
        back_populates="column"
    )
    alias_columns: Mapped[list["AliasColumn"]] = relationship(back_populates="column")


class ColumnRepresentation(Base):
    __tablename__ = "column_representation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("column.id"), nullable=False)
    column_type: Mapped[int] = mapped_column(Integer, nullable=False)
    bits_on_storage: Mapped[int] = mapped_column(Integer, nullable=False)
    first_element_index: Mapped[int | None] = mapped_column(Integer)
    min_value: Mapped[float | None] = mapped_column(Float)
    max_value: Mapped[float | None] = mapped_column(Float)

    column: Mapped[Column] = relationship(back_populates="representations")
    page_groups: Mapped[list["PageGroup"]] = relationship(
        back_populates="column_representation"
    )


class AliasColumn(Base):
    __tablename__ = "alias_column"

    field_id: Mapped[int] = mapped_column(
        ForeignKey("field.id"), primary_key=True, nullable=False
    )
    column_id: Mapped[int] = mapped_column(
        ForeignKey("column.id"), primary_key=True, nullable=False
    )

    field: Mapped[Field] = relationship(back_populates="alias_columns")
    column: Mapped[Column] = relationship(back_populates="alias_columns")


class PageGroup(Base):
    __tablename__ = "page_group"
    __table_args__ = (UniqueConstraint("cluster_id", "column_rep_id"),)

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

    cluster: Mapped[Cluster] = relationship(back_populates="page_groups")
    column_representation: Mapped[ColumnRepresentation] = relationship(
        back_populates="page_groups"
    )
    pages: Mapped[list["Page"]] = relationship(back_populates="page_group")


class Page(Base):
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

    page_bundle: Mapped[PageBundle] = relationship(back_populates="pages")
    page_group: Mapped[PageGroup] = relationship(back_populates="pages")
