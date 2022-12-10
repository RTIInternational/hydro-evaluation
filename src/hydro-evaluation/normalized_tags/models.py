import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    # ForeignKeyConstraint,
    String,
    UniqueConstraint,
    text,
    Table
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy_utils.types.uuid import UUIDType

# declarative base class
Base = declarative_base()

timeseries_string_tag = Table(
    "timeseries_string_tag",
    Base.metadata,
    Column("timeseries_id", ForeignKey("timeseries.id")),
    Column("string_tag_id", ForeignKey("string_tags.id")),
)

timeseries_datetime_tag = Table(
    "timeseries_datetime_tag",
    Base.metadata,
    Column("timeseries_id", ForeignKey("timeseries.id")),
    Column("datetime_tag_id", ForeignKey("datetime_tags.id")),
)

class Timeseries(Base):
    __tablename__ = "timeseries"

    id = Column(UUIDType, primary_key=True, server_default=text(
        "gen_random_uuid()"), default=uuid.uuid4)
    name = Column(String, nullable=False)

    values = relationship(
        "Values",
        back_populates="timeseries",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"Timeseries(id={self.id!r}, name={self.name!r})"


class Values(Base):
    __tablename__ = "values"

    __table_args__ = (
        UniqueConstraint('timeseries_id', 'datetime'),
    )

    id = Column(
        UUIDType,
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid.uuid4
    )
    datetime = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)
    timeseries_id = Column(
        UUIDType,
        ForeignKey("timeseries.id"),
        nullable=False
    )

    timeseries = relationship("Timeseries", back_populates="values")

    def __repr__(self) -> str:
        return f"Values(id={self.id!r}, datetime={self.datetime!r}, value={self.value!r})"


class StringTagTypes(Base):
    __tablename__ = "string_tag_types"

    name = Column(
        String,
        primary_key=True,
        nullable=False
    )

    string_tags = relationship(
        "StringTags",
        back_populates="string_tag_types",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"StringTagTypes(name={self.name!r})"


class StringTags(Base):
    __tablename__ = "string_tags"

    __table_args__ = (
        UniqueConstraint('string_tag_type_name', 'value'),
    )

    id = Column(
        UUIDType,
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid.uuid4
    )
    string_tag_type_name = Column(
        String,
        ForeignKey("string_tag_types.name"),
        nullable=False
    )
    value = Column(String, nullable=False)

    string_tag_types = relationship(
        "StringTagTypes",
        back_populates="string_tags"
    )

    def __repr__(self) -> str:
        return f"StringTags(id={self.id!r}, string_tag_type={self.string_tag_type!r}, value={self.value!r})"


class DateTimeTagTypes(Base):
    __tablename__ = "datetime_tag_types"

    name = Column(
        String,
        primary_key=True,
        nullable=False
    )

    datetime_tags = relationship(
        "DateTimeTags",
        back_populates="datetime_tag_types",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"DateTimeTagTypes(name={self.name!r})"


class DateTimeTags(Base):
    __tablename__ = "datetime_tags"

    __table_args__ = (
        UniqueConstraint('datetime_tag_type_name', 'value'),
    )

    id = Column(
        UUIDType,
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid.uuid4
    )
    datetime_tag_type_name = Column(
        String,
        ForeignKey("datetime_tag_types.name"),
        nullable=False
    )
    value = Column(DateTime, nullable=False)

    datetime_tag_types = relationship(
        "DateTimeTagTypes",
        back_populates="datetime_tags"
    )

    def __repr__(self) -> str:
        return f"DateTimeTags(id={self.id!r}, datetime_tag_type={self.datetime_tag_type!r}, value={self.value!r})"