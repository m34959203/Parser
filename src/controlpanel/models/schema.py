"""SQLAlchemy models for parsing schemas."""

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.controlpanel.database import Base


class ParsingSchemaModel(Base):
    """Parsing schema database model."""

    __tablename__ = "parsing_schemas"

    id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    schema_id: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
    )
    source_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )
    description: Mapped[str] = mapped_column(
        Text,
        default="",
    )
    current_version: Mapped[str] = mapped_column(
        String(20),
        default="1.0.0",
    )

    # Configuration stored as JSON
    start_url: Mapped[str] = mapped_column(Text, nullable=False)
    url_pattern: Mapped[str | None] = mapped_column(Text, nullable=True)
    item_container: Mapped[str | None] = mapped_column(String(500), nullable=True)
    fields: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    navigation_steps: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    pagination: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Quality rules
    min_fields_required: Mapped[int] = mapped_column(default=1)
    dedup_keys: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Execution settings
    mode: Mapped[str] = mapped_column(String(20), default="http")
    requires_js: Mapped[bool] = mapped_column(Boolean, default=False)
    request_headers: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)

    # Metadata
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    versions: Mapped[list["SchemaVersionModel"]] = relationship(
        "SchemaVersionModel",
        back_populates="schema",
        cascade="all, delete-orphan",
        order_by="desc(SchemaVersionModel.created_at)",
    )

    __table_args__ = (
        Index("ix_parsing_schemas_source_active", "source_id", "is_active"),
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "schema_id": self.schema_id,
            "version": self.current_version,
            "source_id": self.source_id,
            "description": self.description,
            "start_url": self.start_url,
            "url_pattern": self.url_pattern,
            "item_container": self.item_container,
            "fields": self.fields,
            "navigation_steps": self.navigation_steps,
            "pagination": self.pagination,
            "min_fields_required": self.min_fields_required,
            "dedup_keys": self.dedup_keys,
            "mode": self.mode,
            "requires_js": self.requires_js,
            "request_headers": self.request_headers,
            "is_active": self.is_active,
            "confidence": self.confidence,
            "tags": self.tags,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class SchemaVersionModel(Base):
    """Schema version history model."""

    __tablename__ = "schema_versions"

    id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    schema_uuid: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    version: Mapped[str] = mapped_column(String(20), nullable=False)

    # Full schema snapshot
    schema_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Metadata
    change_description: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    # Relationship
    schema: Mapped["ParsingSchemaModel"] = relationship(
        "ParsingSchemaModel",
        back_populates="versions",
        foreign_keys=[schema_uuid],
        primaryjoin="SchemaVersionModel.schema_uuid == ParsingSchemaModel.id",
    )

    __table_args__ = (
        Index("ix_schema_versions_schema_version", "schema_uuid", "version", unique=True),
    )
