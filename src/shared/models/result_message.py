"""Result message models for task execution results."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class ExecutionMetrics(BaseModel):
    """Execution performance metrics."""

    duration_ms: int = Field(..., ge=0, description="Total execution time in milliseconds")
    bytes_downloaded: int = Field(default=0, ge=0, description="Total bytes downloaded")
    requests_count: int = Field(default=1, ge=0, description="Number of HTTP requests made")
    pages_processed: int = Field(default=1, ge=0, description="Number of pages processed")
    dns_lookup_ms: int | None = Field(default=None, description="DNS lookup time")
    connection_ms: int | None = Field(default=None, description="Connection establishment time")
    ttfb_ms: int | None = Field(default=None, description="Time to first byte")


class DataPointers(BaseModel):
    """Pointers to stored data locations."""

    delta_path: str = Field(..., description="Path to Delta Lake data")
    raw_html_path: str | None = Field(default=None, description="Path to raw HTML in S3")
    screenshot_path: str | None = Field(default=None, description="Path to screenshot in S3")
    hudi_path: str | None = Field(default=None, description="Path to Hudi technical data")
    artifacts: dict[str, str] = Field(
        default_factory=dict, description="Additional artifact paths"
    )


class ErrorDetail(BaseModel):
    """Detailed error information."""

    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Human-readable error message")
    is_retryable: bool = Field(default=True, description="Whether error is retryable")
    stack_trace: str | None = Field(default=None, description="Stack trace if available")
    context: dict = Field(default_factory=dict, description="Error context data")

    # Common error codes
    class Codes:
        TIMEOUT = "TIMEOUT"
        CONNECTION_ERROR = "CONNECTION_ERROR"
        HTTP_ERROR = "HTTP_ERROR"
        PROXY_ERROR = "PROXY_ERROR"
        SELECTOR_NOT_FOUND = "SELECTOR_NOT_FOUND"
        VALIDATION_ERROR = "VALIDATION_ERROR"
        RATE_LIMITED = "RATE_LIMITED"
        BLOCKED = "BLOCKED"
        CAPTCHA = "CAPTCHA"
        AUTH_REQUIRED = "AUTH_REQUIRED"
        PARSE_ERROR = "PARSE_ERROR"
        UNKNOWN = "UNKNOWN"


class ExtractionStats(BaseModel):
    """Statistics about data extraction."""

    records_extracted: int = Field(default=0, ge=0, description="Total records extracted")
    records_valid: int = Field(default=0, ge=0, description="Records passing validation")
    records_rejected: int = Field(default=0, ge=0, description="Records rejected/invalid")
    records_deduplicated: int = Field(default=0, ge=0, description="Duplicate records removed")
    fields_extracted: dict[str, int] = Field(
        default_factory=dict, description="Count per field extracted"
    )
    fields_missing: dict[str, int] = Field(
        default_factory=dict, description="Count per field missing"
    )


class ResultMessage(BaseModel):
    """Result message for RabbitMQ results queue."""

    # Identifiers
    task_id: UUID = Field(..., description="Original task identifier")
    run_id: UUID = Field(..., description="Execution run identifier")

    # Status
    status: Literal["success", "partial", "failed", "retry"] = Field(
        ..., description="Execution result status"
    )
    http_status: int | None = Field(default=None, description="Final HTTP status code")

    # Performance metrics
    metrics: ExecutionMetrics = Field(..., description="Execution metrics")

    # Data locations
    pointers: DataPointers = Field(..., description="Data storage pointers")

    # Extraction statistics
    extraction: ExtractionStats = Field(
        default_factory=ExtractionStats, description="Extraction statistics"
    )

    # Pagination info
    has_next_page: bool = Field(default=False, description="Whether more pages exist")
    next_page_url: str | None = Field(default=None, description="URL of next page if any")
    current_page: int = Field(default=1, description="Current page number")

    # Errors
    errors: list[ErrorDetail] = Field(default_factory=list, description="List of errors")

    # Timestamps
    started_at: datetime = Field(..., description="Execution start time")
    completed_at: datetime = Field(default_factory=datetime.utcnow, description="Completion time")

    # Debug info
    worker_id: str | None = Field(default=None, description="Worker that processed the task")
    debug_info: dict = Field(default_factory=dict, description="Debug information")

    @property
    def is_success(self) -> bool:
        """Check if execution was successful."""
        return self.status in ("success", "partial")

    @property
    def should_retry(self) -> bool:
        """Check if task should be retried."""
        if self.status != "failed":
            return False
        return any(e.is_retryable for e in self.errors)

    model_config = {
        "json_schema_extra": {
            "example": {
                "task_id": "550e8400-e29b-41d4-a716-446655440000",
                "run_id": "660e8400-e29b-41d4-a716-446655440001",
                "status": "success",
                "http_status": 200,
                "metrics": {
                    "duration_ms": 1250,
                    "bytes_downloaded": 45000,
                    "requests_count": 1,
                    "pages_processed": 1,
                },
                "pointers": {
                    "delta_path": "s3://parser-lake/delta/example.com/2024/01/15/task-550e8400/",
                },
                "extraction": {
                    "records_extracted": 24,
                    "records_valid": 24,
                    "records_rejected": 0,
                },
                "started_at": "2024-01-15T10:30:00Z",
                "completed_at": "2024-01-15T10:30:01.250Z",
            }
        }
    }
