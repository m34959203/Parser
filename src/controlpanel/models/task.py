"""SQLAlchemy models for tasks."""

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Enum, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.controlpanel.database import Base
from src.shared.models import TaskStatus


class TaskModel(Base):
    """Task database model."""

    __tablename__ = "tasks"

    id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    source_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    schema_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    schema_version: Mapped[str] = mapped_column(String(20), default="latest")
    mode: Mapped[str] = mapped_column(String(20), default="http")

    # Status
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus),
        default=TaskStatus.PENDING,
        index=True,
    )

    # Priority and limits
    priority: Mapped[int] = mapped_column(Integer, default=5)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    current_attempt: Mapped[int] = mapped_column(Integer, default=0)

    # Profiles
    proxy_profile_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    session_profile_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Context
    context: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    parent_task_id: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
    )

    # Results
    records_extracted: Mapped[int] = mapped_column(Integer, default=0)
    records_valid: Mapped[int] = mapped_column(Integer, default=0)
    delta_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    errors: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    runs: Mapped[list["TaskRunModel"]] = relationship(
        "TaskRunModel",
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="desc(TaskRunModel.created_at)",
    )

    __table_args__ = (
        Index("ix_tasks_status_created", "status", "created_at"),
        Index("ix_tasks_source_status", "source_id", "status"),
    )


class TaskRunModel(Base):
    """Task run/attempt model."""

    __tablename__ = "task_runs"

    id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    run_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        default=uuid4,
        unique=True,
    )
    attempt: Mapped[int] = mapped_column(Integer, default=1)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="running")
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Metrics
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bytes_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    requests_count: Mapped[int] = mapped_column(Integer, default=0)
    pages_processed: Mapped[int] = mapped_column(Integer, default=0)

    # Results
    records_extracted: Mapped[int] = mapped_column(Integer, default=0)
    records_valid: Mapped[int] = mapped_column(Integer, default=0)
    records_rejected: Mapped[int] = mapped_column(Integer, default=0)

    # Data pointers
    delta_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_html_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    screenshot_path: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Errors
    errors: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    # Worker info
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationship
    task: Mapped["TaskModel"] = relationship(
        "TaskModel",
        back_populates="runs",
        foreign_keys=[task_id],
        primaryjoin="TaskRunModel.task_id == TaskModel.id",
    )

    __table_args__ = (
        Index("ix_task_runs_task_attempt", "task_id", "attempt"),
    )
