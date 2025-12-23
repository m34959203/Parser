"""Result builder for UCA workers."""

from datetime import datetime
from typing import Any
from uuid import UUID

from src.shared.models import (
    DataPointers,
    ErrorDetail,
    ExecutionMetrics,
    ExtractionStats,
    ResultMessage,
)


class ResultBuilder:
    """Builder for constructing result messages."""

    def __init__(self, task_id: UUID, run_id: UUID):
        self.task_id = task_id
        self.run_id = run_id
        self._started_at: datetime = datetime.utcnow()
        self._status: str = "running"
        self._http_status: int | None = None
        self._errors: list[ErrorDetail] = []
        self._records_extracted: int = 0
        self._records_valid: int = 0
        self._records_rejected: int = 0
        self._fields_extracted: dict[str, int] = {}
        self._fields_missing: dict[str, int] = {}
        self._bytes_downloaded: int = 0
        self._requests_count: int = 0
        self._pages_processed: int = 0
        self._delta_path: str = ""
        self._raw_html_path: str | None = None
        self._screenshot_path: str | None = None
        self._artifacts: dict[str, str] = {}
        self._has_next_page: bool = False
        self._next_page_url: str | None = None
        self._current_page: int = 1
        self._worker_id: str | None = None
        self._debug_info: dict[str, Any] = {}

    def set_started(self) -> "ResultBuilder":
        """Mark task as started."""
        self._started_at = datetime.utcnow()
        return self

    def set_http_status(self, status: int) -> "ResultBuilder":
        """Set HTTP response status code."""
        self._http_status = status
        return self

    def add_error(
        self,
        code: str,
        message: str,
        is_retryable: bool = True,
        stack_trace: str | None = None,
        context: dict | None = None,
    ) -> "ResultBuilder":
        """Add an error to the result."""
        self._errors.append(
            ErrorDetail(
                code=code,
                message=message,
                is_retryable=is_retryable,
                stack_trace=stack_trace,
                context=context or {},
            )
        )
        return self

    def add_bytes_downloaded(self, bytes_count: int) -> "ResultBuilder":
        """Add to bytes downloaded counter."""
        self._bytes_downloaded += bytes_count
        return self

    def increment_requests(self, count: int = 1) -> "ResultBuilder":
        """Increment request counter."""
        self._requests_count += count
        return self

    def increment_pages(self, count: int = 1) -> "ResultBuilder":
        """Increment pages processed counter."""
        self._pages_processed += count
        return self

    def set_extraction_stats(
        self,
        extracted: int,
        valid: int,
        rejected: int = 0,
        fields_extracted: dict[str, int] | None = None,
        fields_missing: dict[str, int] | None = None,
    ) -> "ResultBuilder":
        """Set extraction statistics."""
        self._records_extracted = extracted
        self._records_valid = valid
        self._records_rejected = rejected
        if fields_extracted:
            self._fields_extracted = fields_extracted
        if fields_missing:
            self._fields_missing = fields_missing
        return self

    def set_delta_path(self, path: str) -> "ResultBuilder":
        """Set Delta Lake data path."""
        self._delta_path = path
        return self

    def set_raw_html_path(self, path: str) -> "ResultBuilder":
        """Set raw HTML storage path."""
        self._raw_html_path = path
        return self

    def set_screenshot_path(self, path: str) -> "ResultBuilder":
        """Set screenshot storage path."""
        self._screenshot_path = path
        return self

    def add_artifact(self, name: str, path: str) -> "ResultBuilder":
        """Add an artifact path."""
        self._artifacts[name] = path
        return self

    def set_pagination(
        self,
        has_next: bool,
        next_url: str | None = None,
        current_page: int = 1,
    ) -> "ResultBuilder":
        """Set pagination information."""
        self._has_next_page = has_next
        self._next_page_url = next_url
        self._current_page = current_page
        return self

    def set_worker_id(self, worker_id: str) -> "ResultBuilder":
        """Set worker identifier."""
        self._worker_id = worker_id
        return self

    def add_debug_info(self, key: str, value: Any) -> "ResultBuilder":
        """Add debug information."""
        self._debug_info[key] = value
        return self

    def build_success(self) -> ResultMessage:
        """Build a successful result message."""
        self._status = "success" if self._records_valid > 0 else "partial"
        return self._build()

    def build_partial(self) -> ResultMessage:
        """Build a partial success result message."""
        self._status = "partial"
        return self._build()

    def build_failed(self) -> ResultMessage:
        """Build a failed result message."""
        self._status = "failed"
        return self._build()

    def build_retry(self) -> ResultMessage:
        """Build a retry result message."""
        self._status = "retry"
        return self._build()

    def _build(self) -> ResultMessage:
        """Build the final result message."""
        completed_at = datetime.utcnow()
        duration_ms = int((completed_at - self._started_at).total_seconds() * 1000)

        return ResultMessage(
            task_id=self.task_id,
            run_id=self.run_id,
            status=self._status,
            http_status=self._http_status,
            metrics=ExecutionMetrics(
                duration_ms=duration_ms,
                bytes_downloaded=self._bytes_downloaded,
                requests_count=self._requests_count,
                pages_processed=self._pages_processed,
            ),
            pointers=DataPointers(
                delta_path=self._delta_path,
                raw_html_path=self._raw_html_path,
                screenshot_path=self._screenshot_path,
                artifacts=self._artifacts,
            ),
            extraction=ExtractionStats(
                records_extracted=self._records_extracted,
                records_valid=self._records_valid,
                records_rejected=self._records_rejected,
                fields_extracted=self._fields_extracted,
                fields_missing=self._fields_missing,
            ),
            has_next_page=self._has_next_page,
            next_page_url=self._next_page_url,
            current_page=self._current_page,
            errors=self._errors,
            started_at=self._started_at,
            completed_at=completed_at,
            worker_id=self._worker_id,
            debug_info=self._debug_info,
        )
