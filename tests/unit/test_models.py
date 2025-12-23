"""
Unit tests for shared Pydantic models.
"""
import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from src.shared.models.parsing_schema import (
    ParsingSchema,
    ParsingSchemaCreate,
    ParsingSchemaUpdate,
    FieldDefinition,
    NavigationStep,
    PaginationRule,
    FieldType,
    ExtractionMethod,
)
from src.shared.models.task_message import (
    TaskMessage,
    TaskCreate,
    TaskStatus,
    TaskMode,
)
from src.shared.models.result_message import (
    ResultMessage,
    ExecutionMetrics,
    DataPointers,
    ErrorDetail,
    ResultStatus,
)


class TestFieldDefinition:
    """Tests for FieldDefinition model."""

    def test_valid_field_definition(self):
        """Test creating a valid field definition."""
        field = FieldDefinition(
            name="title",
            type=FieldType.STRING,
            method=ExtractionMethod.CSS,
            selector="h1.product-title",
            required=True,
            transformations=["trim", "lowercase"],
        )

        assert field.name == "title"
        assert field.selector == "h1.product-title"
        assert field.type == FieldType.STRING
        assert field.method == ExtractionMethod.CSS
        assert field.required is True
        assert field.transformations == ["trim", "lowercase"]

    def test_field_with_fallback_selectors(self):
        """Test field definition with fallback selectors."""
        field = FieldDefinition(
            name="price",
            selector=".price-main",
            fallback_selectors=[".price", ".cost", "span[data-price]"],
        )

        assert len(field.fallback_selectors) == 3
        assert field.fallback_selectors[0] == ".price"

    def test_field_default_values(self):
        """Test field definition with default values."""
        field = FieldDefinition(
            name="test",
            selector=".test",
        )

        assert field.type == FieldType.STRING
        assert field.method == ExtractionMethod.CSS
        assert field.attribute is None
        assert field.default is None
        assert field.required is True
        assert field.transformations == []
        assert field.validation_regex is None
        assert field.fallback_selectors == []

    def test_field_with_validation_regex(self):
        """Test field with validation regex."""
        field = FieldDefinition(
            name="email",
            selector=".email",
            validation_regex=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        )

        assert field.validation_regex is not None

    def test_empty_name_raises_error(self):
        """Test that empty field name raises validation error."""
        with pytest.raises(ValidationError):
            FieldDefinition(
                name="",
                selector=".test",
            )

    def test_empty_selector_raises_error(self):
        """Test that empty selector raises validation error."""
        with pytest.raises(ValidationError):
            FieldDefinition(
                name="test",
                selector="",
            )


class TestNavigationStep:
    """Tests for NavigationStep model."""

    def test_click_action(self):
        """Test click navigation step."""
        step = NavigationStep(
            action="click",
            target=".button",
            wait_ms=1000,
        )

        assert step.action == "click"
        assert step.target == ".button"
        assert step.wait_ms == 1000

    def test_input_action(self):
        """Test input navigation step."""
        step = NavigationStep(
            action="input",
            target="#search",
            value="search query",
        )

        assert step.action == "input"
        assert step.value == "search query"

    def test_scroll_action(self):
        """Test scroll navigation step."""
        step = NavigationStep(
            action="scroll",
            wait_ms=500,
        )

        assert step.action == "scroll"
        assert step.wait_ms == 500

    def test_wait_for_element(self):
        """Test navigation step with wait_for."""
        step = NavigationStep(
            action="click",
            target=".load-more",
            wait_for=".products-loaded",
            wait_ms=2000,
        )

        assert step.wait_for == ".products-loaded"

    def test_optional_step(self):
        """Test optional navigation step."""
        step = NavigationStep(
            action="click",
            target=".dismiss-popup",
            optional=True,
        )

        assert step.optional is True


class TestPaginationRule:
    """Tests for PaginationRule model."""

    def test_next_button_pagination(self):
        """Test next button pagination rule."""
        rule = PaginationRule(
            type="next_button",
            selector=".next-page",
            max_pages=10,
        )

        assert rule.type == "next_button"
        assert rule.selector == ".next-page"
        assert rule.max_pages == 10

    def test_infinite_scroll_pagination(self):
        """Test infinite scroll pagination rule."""
        rule = PaginationRule(
            type="infinite_scroll",
            max_pages=5,
            scroll_delay_ms=1500,
        )

        assert rule.type == "infinite_scroll"
        assert rule.max_pages == 5
        assert rule.scroll_delay_ms == 1500

    def test_page_param_pagination(self):
        """Test page parameter pagination."""
        rule = PaginationRule(
            type="page_param",
            param_name="page",
            param_start=1,
            param_step=1,
            max_pages=20,
        )

        assert rule.type == "page_param"
        assert rule.param_name == "page"

    def test_stop_selector(self):
        """Test pagination with stop selector."""
        rule = PaginationRule(
            type="next_button",
            selector=".next",
            stop_selector=".last-page",
            max_pages=100,
        )

        assert rule.stop_selector == ".last-page"


class TestParsingSchema:
    """Tests for ParsingSchema model."""

    def test_valid_schema(self):
        """Test creating a valid parsing schema."""
        schema = ParsingSchema(
            schema_id="test_schema",
            version="1.0.0",
            source_id="test.com",
            description="Test schema",
            start_url="https://test.com/catalog",
            fields=[
                FieldDefinition(name="title", selector=".title"),
                FieldDefinition(name="price", selector=".price"),
            ],
        )

        assert schema.schema_id == "test_schema"
        assert schema.version == "1.0.0"
        assert schema.source_id == "test.com"
        assert len(schema.fields) == 2
        assert schema.mode == "http"

    def test_schema_with_navigation_and_pagination(self):
        """Test schema with navigation and pagination rules."""
        schema = ParsingSchema(
            schema_id="full_schema",
            source_id="test",
            start_url="https://test.com",
            fields=[FieldDefinition(name="data", selector=".data")],
            navigation_steps=[
                NavigationStep(action="click", target=".load-more"),
            ],
            pagination=PaginationRule(
                type="next_button",
                selector=".next",
                max_pages=5,
            ),
            requires_js=True,
        )

        assert len(schema.navigation_steps) == 1
        assert schema.pagination is not None
        assert schema.pagination.type == "next_button"
        assert schema.requires_js is True

    def test_schema_default_values(self):
        """Test schema default values."""
        schema = ParsingSchema(
            schema_id="minimal",
            source_id="test",
            start_url="https://test.com",
            fields=[FieldDefinition(name="field", selector=".field")],
        )

        assert schema.version == "1.0.0"
        assert schema.description == ""
        assert schema.url_pattern is None
        assert schema.navigation_steps == []
        assert schema.pagination is None
        assert schema.item_container is None
        assert schema.min_fields_required == 1
        assert schema.dedup_keys == []
        assert schema.mode == "http"
        assert schema.requires_js is False
        assert schema.request_headers == {}
        assert schema.is_active is True
        assert schema.tags == []

    def test_schema_requires_at_least_one_field(self):
        """Test that schema requires at least one field."""
        with pytest.raises(ValidationError):
            ParsingSchema(
                schema_id="empty",
                source_id="test",
                start_url="https://test.com",
                fields=[],
            )

    def test_schema_unique_field_names(self):
        """Test that field names must be unique."""
        with pytest.raises(ValidationError):
            ParsingSchema(
                schema_id="duplicate_fields",
                source_id="test",
                start_url="https://test.com",
                fields=[
                    FieldDefinition(name="title", selector=".title"),
                    FieldDefinition(name="title", selector=".other"),
                ],
            )

    def test_schema_dedup_keys_validation(self):
        """Test that dedup keys reference existing fields."""
        with pytest.raises(ValidationError):
            ParsingSchema(
                schema_id="bad_dedup",
                source_id="test",
                start_url="https://test.com",
                fields=[FieldDefinition(name="title", selector=".title")],
                dedup_keys=["nonexistent_field"],
            )


class TestTaskMessage:
    """Tests for TaskMessage model."""

    def test_valid_task_message(self):
        """Test creating a valid task message."""
        task = TaskMessage(
            task_id="task-001",
            source_id="test-source",
            schema_id="schema-001",
            target_url="https://example.com/product/123",
            mode=TaskMode.HTTP,
            priority=5,
            attempt=1,
            max_attempts=3,
        )

        assert task.task_id == "task-001"
        assert task.source_id == "test-source"
        assert task.schema_id == "schema-001"
        assert task.mode == TaskMode.HTTP
        assert task.priority == 5
        assert task.attempt == 1

    def test_task_default_values(self):
        """Test task message default values."""
        task = TaskMessage(
            task_id="task-002",
            source_id="src",
            schema_id="schema",
            target_url="https://test.com",
        )

        assert task.mode == TaskMode.HTTP
        assert task.priority == 5
        assert task.attempt == 1
        assert task.max_attempts == 3
        assert task.callback_url is None
        assert task.metadata == {}

    def test_task_priority_range(self):
        """Test task priority validation."""
        for priority in [1, 5, 10]:
            task = TaskMessage(
                task_id="task",
                source_id="src",
                schema_id="schema",
                target_url="https://test.com",
                priority=priority,
            )
            assert task.priority == priority

    def test_task_mode_validation(self):
        """Test task mode validation."""
        task = TaskMessage(
            task_id="task",
            source_id="src",
            schema_id="schema",
            target_url="https://test.com",
            mode="browser",
        )
        assert task.mode == TaskMode.BROWSER


class TestTaskCreate:
    """Tests for TaskCreate model."""

    def test_valid_task_create(self):
        """Test creating a valid task create request."""
        task = TaskCreate(
            source_id="source",
            schema_id="schema",
            target_url="https://example.com/page",
        )

        assert task.source_id == "source"
        assert task.schema_id == "schema"
        assert task.target_url == "https://example.com/page"
        assert task.mode == TaskMode.HTTP

    def test_task_create_with_all_fields(self):
        """Test task create with all optional fields."""
        task = TaskCreate(
            source_id="source",
            schema_id="schema",
            target_url="https://example.com/page",
            mode="browser",
            priority=1,
            max_attempts=5,
            callback_url="https://webhook.example.com",
            metadata={"key": "value"},
        )

        assert task.mode == TaskMode.BROWSER
        assert task.priority == 1
        assert task.max_attempts == 5
        assert task.callback_url == "https://webhook.example.com"
        assert task.metadata == {"key": "value"}


class TestResultMessage:
    """Tests for ResultMessage model."""

    def test_success_result(self):
        """Test creating a success result message."""
        result = ResultMessage(
            task_id="task-001",
            source_id="source",
            schema_id="schema",
            status=ResultStatus.SUCCESS,
            records_extracted=10,
            data_pointers=DataPointers(
                bronze_path="s3://bucket/bronze/data",
                silver_path="s3://bucket/silver/data",
            ),
            execution_metrics=ExecutionMetrics(
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc),
                duration_ms=1500,
                bytes_downloaded=50000,
                requests_made=5,
            ),
        )

        assert result.status == ResultStatus.SUCCESS
        assert result.records_extracted == 10
        assert result.data_pointers.bronze_path == "s3://bucket/bronze/data"

    def test_failed_result_with_errors(self):
        """Test creating a failed result with errors."""
        result = ResultMessage(
            task_id="task-002",
            source_id="source",
            schema_id="schema",
            status=ResultStatus.FAILED,
            records_extracted=0,
            errors=[
                ErrorDetail(
                    code="HTTP_ERROR",
                    message="Connection timeout",
                    timestamp=datetime.now(timezone.utc),
                    recoverable=True,
                ),
            ],
        )

        assert result.status == ResultStatus.FAILED
        assert len(result.errors) == 1
        assert result.errors[0].code == "HTTP_ERROR"
        assert result.errors[0].recoverable is True

    def test_partial_result(self):
        """Test creating a partial success result."""
        result = ResultMessage(
            task_id="task-003",
            source_id="source",
            schema_id="schema",
            status=ResultStatus.PARTIAL,
            records_extracted=5,
            errors=[
                ErrorDetail(
                    code="EXTRACTION_ERROR",
                    message="Some fields could not be extracted",
                    timestamp=datetime.now(timezone.utc),
                    recoverable=False,
                ),
            ],
        )

        assert result.status == ResultStatus.PARTIAL
        assert result.records_extracted == 5
        assert len(result.errors) == 1


class TestExecutionMetrics:
    """Tests for ExecutionMetrics model."""

    def test_execution_metrics(self):
        """Test creating execution metrics."""
        start = datetime.now(timezone.utc)
        metrics = ExecutionMetrics(
            start_time=start,
            end_time=start,
            duration_ms=2500,
            bytes_downloaded=100000,
            requests_made=10,
            pages_processed=3,
            retries=1,
        )

        assert metrics.duration_ms == 2500
        assert metrics.bytes_downloaded == 100000
        assert metrics.requests_made == 10
        assert metrics.pages_processed == 3
        assert metrics.retries == 1

    def test_execution_metrics_defaults(self):
        """Test execution metrics default values."""
        metrics = ExecutionMetrics(
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_ms=1000,
        )

        assert metrics.bytes_downloaded == 0
        assert metrics.requests_made == 1
        assert metrics.pages_processed == 1
        assert metrics.retries == 0


class TestParsingSchemaCreate:
    """Tests for ParsingSchemaCreate model."""

    def test_valid_schema_create(self):
        """Test creating a valid schema create request."""
        schema = ParsingSchemaCreate(
            source_id="test.com",
            start_url="https://test.com/products",
            fields=[FieldDefinition(name="title", selector=".title")],
        )

        assert schema.source_id == "test.com"
        assert schema.start_url == "https://test.com/products"
        assert len(schema.fields) == 1


class TestParsingSchemaUpdate:
    """Tests for ParsingSchemaUpdate model."""

    def test_partial_update(self):
        """Test partial schema update."""
        update = ParsingSchemaUpdate(
            description="Updated description",
            is_active=False,
        )

        assert update.description == "Updated description"
        assert update.is_active is False
        assert update.start_url is None
        assert update.fields is None
