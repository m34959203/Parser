"""Task message models for RabbitMQ communication."""

from datetime import datetime
from enum import Enum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"
    DLQ = "dlq"


class TaskPriority(int, Enum):
    """Task priority levels."""

    LOWEST = 0
    LOW = 2
    NORMAL = 5
    HIGH = 7
    URGENT = 9
    CRITICAL = 10


class TaskMessage(BaseModel):
    """Task message for RabbitMQ queue."""

    # Identifiers
    task_id: UUID = Field(default_factory=uuid4, description="Unique task identifier")
    run_id: UUID = Field(default_factory=uuid4, description="Execution run identifier")
    branch_id: str | None = Field(default=None, description="Branch/scenario identifier")
    parent_task_id: UUID | None = Field(default=None, description="Parent task for sub-tasks")

    # Source and target
    source_id: str = Field(..., description="Source identifier")
    target_url: str = Field(..., description="URL to scrape")
    mode: Literal["http", "browser"] = Field(default="http", description="Execution mode")

    # Schema reference
    schema_id: str = Field(..., description="Parsing schema ID")
    schema_version: str = Field(default="latest", description="Schema version")

    # Priority and limits
    priority: int = Field(default=TaskPriority.NORMAL, ge=0, le=10, description="Task priority")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")
    ttl_seconds: int = Field(default=3600, ge=60, le=86400, description="Time-to-live in seconds")
    timeout_seconds: int = Field(default=60, ge=10, le=600, description="Execution timeout")

    # Profile references
    proxy_profile_id: str | None = Field(default=None, description="Proxy profile identifier")
    session_profile_id: str | None = Field(default=None, description="Browser session profile")

    # Execution context
    context: dict = Field(default_factory=dict, description="Additional context data")
    cookies: list[dict] | None = Field(default=None, description="Cookies to use")
    headers: dict[str, str] = Field(default_factory=dict, description="Custom HTTP headers")

    # Pagination context
    page_number: int = Field(default=1, description="Current page number")
    max_pages: int | None = Field(default=None, description="Override max pages")

    # Timestamps and counters
    created_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_at: datetime | None = Field(default=None, description="Scheduled execution time")
    attempt: int = Field(default=0, description="Current attempt number")

    def next_attempt(self) -> "TaskMessage":
        """Create a copy for the next retry attempt."""
        return self.model_copy(
            update={
                "run_id": uuid4(),
                "attempt": self.attempt + 1,
            }
        )

    def child_task(self, target_url: str, **kwargs) -> "TaskMessage":
        """Create a child task for pagination or sub-pages."""
        return TaskMessage(
            source_id=self.source_id,
            target_url=target_url,
            mode=self.mode,
            schema_id=self.schema_id,
            schema_version=self.schema_version,
            priority=self.priority,
            max_attempts=self.max_attempts,
            proxy_profile_id=self.proxy_profile_id,
            session_profile_id=self.session_profile_id,
            parent_task_id=self.task_id,
            branch_id=self.branch_id,
            context=self.context,
            cookies=self.cookies,
            headers=self.headers,
            **kwargs,
        )

    model_config = {
        "json_schema_extra": {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "source_id": "example.com/products",
                "target_url": "https://example.com/catalog?page=1",
                "mode": "http",
                "schema_id": "example_products_v1",
                "priority": 5,
                "max_attempts": 3,
            }
        }
    }


class TaskCreate(BaseModel):
    """Request model for creating a new task."""

    source_id: str = Field(..., min_length=1)
    target_url: str = Field(...)
    schema_id: str = Field(..., min_length=1)
    schema_version: str = Field(default="latest")
    mode: Literal["http", "browser"] = "http"
    priority: int = Field(default=5, ge=0, le=10)
    max_attempts: int = Field(default=3, ge=1, le=10)
    proxy_profile_id: str | None = None
    session_profile_id: str | None = None
    context: dict = Field(default_factory=dict)
    scheduled_at: datetime | None = None
    max_pages: int | None = None


class TaskResponse(BaseModel):
    """Response model for task operations."""

    task_id: UUID
    status: TaskStatus
    message: str
    created_at: datetime


class TaskDetail(BaseModel):
    """Detailed task information."""

    task_id: UUID
    run_id: UUID | None
    source_id: str
    target_url: str
    schema_id: str
    mode: str
    status: TaskStatus
    priority: int
    attempt: int
    max_attempts: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    records_extracted: int = 0
    errors: list[str] = Field(default_factory=list)


class TaskListResponse(BaseModel):
    """Paginated task list response."""

    items: list[TaskDetail]
    total: int
    limit: int
    offset: int
