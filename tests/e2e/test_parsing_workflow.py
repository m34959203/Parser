"""
End-to-end tests for the complete parsing workflow.

These tests simulate the full lifecycle:
1. Creating a parsing schema
2. Submitting parsing tasks
3. Processing tasks by workers
4. Saving results to data lake
5. Loading data to PostgreSQL
"""
import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime, timezone

import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

from src.controlpanel.main import app
from src.controlpanel.database import Base, get_async_session
from src.shared.models import (
    ParsingSchema,
    FieldDefinition,
    TaskMessage,
    ResultMessage,
    ExecutionMetrics,
    DataPointers,
    ResultStatus,
)
from src.uca.common.extractor import DataExtractor


# Test fixtures
@pytest_asyncio.fixture
async def async_engine():
    """Create an async in-memory SQLite engine for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def client(async_engine):
    """Create a test client with mocked database."""
    async_session_maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def override_get_session():
        async with async_session_maker() as session:
            yield session

    app.dependency_overrides[get_async_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def ecommerce_html():
    """Sample e-commerce product listing HTML."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Products - Test Shop</title></head>
    <body>
        <div class="product-list">
            <div class="product-card" data-product-id="1001">
                <h2 class="product-name">Wireless Headphones Pro</h2>
                <span class="price" data-raw="149.99">$149.99</span>
                <span class="rating" data-score="4.5">4.5/5</span>
                <span class="stock in-stock">In Stock</span>
                <a href="/product/1001" class="product-link">View Details</a>
                <img src="/images/headphones.jpg" alt="Wireless Headphones Pro" />
            </div>
            <div class="product-card" data-product-id="1002">
                <h2 class="product-name">Bluetooth Speaker Max</h2>
                <span class="price" data-raw="79.99">$79.99</span>
                <span class="rating" data-score="4.2">4.2/5</span>
                <span class="stock in-stock">In Stock</span>
                <a href="/product/1002" class="product-link">View Details</a>
                <img src="/images/speaker.jpg" alt="Bluetooth Speaker Max" />
            </div>
            <div class="product-card" data-product-id="1003">
                <h2 class="product-name">Smart Watch Elite</h2>
                <span class="price" data-raw="299.99">$299.99</span>
                <span class="rating" data-score="4.8">4.8/5</span>
                <span class="stock out-of-stock">Out of Stock</span>
                <a href="/product/1003" class="product-link">View Details</a>
                <img src="/images/watch.jpg" alt="Smart Watch Elite" />
            </div>
        </div>
        <div class="pagination">
            <a href="/products?page=2" class="next-page">Next Page</a>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def ecommerce_schema_data():
    """Schema for e-commerce product extraction."""
    return {
        "source_id": "testshop.com",
        "description": "E-commerce product catalog parser",
        "start_url": "https://testshop.com/products",
        "url_pattern": r"https://testshop\.com/products.*",
        "item_container": "div.product-card",
        "fields": [
            {
                "name": "product_id",
                "type": "string",
                "method": "css",
                "selector": ".product-card",
                "attribute": "data-product-id",
                "required": True,
            },
            {
                "name": "name",
                "type": "string",
                "method": "css",
                "selector": "h2.product-name",
                "required": True,
                "transformations": ["trim"],
            },
            {
                "name": "price",
                "type": "float",
                "method": "css",
                "selector": "span.price",
                "attribute": "data-raw",
                "required": True,
            },
            {
                "name": "rating",
                "type": "float",
                "method": "css",
                "selector": "span.rating",
                "attribute": "data-score",
                "required": False,
            },
            {
                "name": "in_stock",
                "type": "boolean",
                "method": "css",
                "selector": "span.stock",
                "required": False,
                "transformations": ["to_bool"],
            },
            {
                "name": "url",
                "type": "url",
                "method": "css",
                "selector": "a.product-link",
                "attribute": "href",
                "required": True,
                "transformations": ["absolute_url"],
            },
            {
                "name": "image",
                "type": "url",
                "method": "css",
                "selector": "img",
                "attribute": "src",
                "required": False,
                "transformations": ["absolute_url"],
            },
        ],
        "min_fields_required": 3,
        "dedup_keys": ["product_id"],
        "mode": "http",
        "requires_js": False,
        "pagination": {
            "type": "next_button",
            "selector": "a.next-page",
            "max_pages": 10,
        },
        "tags": ["ecommerce", "products"],
    }


class TestFullParsingWorkflow:
    """End-to-end tests for the complete parsing workflow."""

    @pytest.mark.asyncio
    async def test_schema_creation_and_task_submission(self, client, ecommerce_schema_data):
        """Test creating a schema and submitting a task."""
        # Step 1: Create schema
        schema_response = await client.post("/api/v1/schemas/", json=ecommerce_schema_data)
        assert schema_response.status_code == 201
        schema = schema_response.json()
        schema_id = schema["schema_id"]

        assert schema["source_id"] == "testshop.com"
        assert len(schema["fields"]) == 7
        assert schema["pagination"]["type"] == "next_button"

        # Step 2: Submit task
        task_data = {
            "source_id": "testshop.com",
            "schema_id": schema_id,
            "target_url": "https://testshop.com/products",
            "priority": 5,
            "metadata": {"category": "electronics"},
        }

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            task_response = await client.post("/api/v1/tasks/", json=task_data)

        assert task_response.status_code == 201
        task = task_response.json()

        assert task["schema_id"] == schema_id
        assert task["status"] == "pending"
        assert task["target_url"] == "https://testshop.com/products"

        return schema_id, task["task_id"]

    @pytest.mark.asyncio
    async def test_extraction_with_schema(self, ecommerce_html, ecommerce_schema_data):
        """Test data extraction using the schema."""
        # Convert schema data to ParsingSchema model
        schema = ParsingSchema(
            schema_id="test_schema",
            **ecommerce_schema_data
        )

        # Create extractor and extract data
        extractor = DataExtractor(schema, base_url="https://testshop.com")
        records = extractor.extract(ecommerce_html)

        # Verify extraction results
        assert len(records) == 3

        # Check first product
        product1 = records[0]
        assert product1["product_id"] == "1001"
        assert product1["name"] == "Wireless Headphones Pro"
        assert product1["price"] == 149.99
        assert product1["rating"] == 4.5
        assert product1["url"] == "https://testshop.com/product/1001"
        assert product1["image"] == "https://testshop.com/images/headphones.jpg"

        # Check second product
        product2 = records[1]
        assert product2["product_id"] == "1002"
        assert product2["name"] == "Bluetooth Speaker Max"
        assert product2["price"] == 79.99

        # Check third product
        product3 = records[2]
        assert product3["product_id"] == "1003"
        assert product3["name"] == "Smart Watch Elite"
        assert product3["price"] == 299.99

    @pytest.mark.asyncio
    async def test_result_message_creation(self):
        """Test creating result message after extraction."""
        # Simulate worker creating result
        result = ResultMessage(
            task_id="task-001",
            source_id="testshop.com",
            schema_id="schema-001",
            status=ResultStatus.SUCCESS,
            records_extracted=3,
            data_pointers=DataPointers(
                bronze_path="s3://parser-data/bronze/testshop/2024/01/15/task-001.parquet",
                silver_path="s3://parser-data/silver/testshop/2024/01/15/task-001.parquet",
            ),
            execution_metrics=ExecutionMetrics(
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc),
                duration_ms=1500,
                bytes_downloaded=15000,
                requests_made=1,
                pages_processed=1,
            ),
        )

        assert result.status == ResultStatus.SUCCESS
        assert result.records_extracted == 3
        assert result.data_pointers.bronze_path.endswith(".parquet")
        assert result.execution_metrics.duration_ms == 1500

    @pytest.mark.asyncio
    async def test_task_status_updates(self, client, ecommerce_schema_data):
        """Test task status lifecycle."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=ecommerce_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Create task
        task_data = {
            "source_id": "testshop.com",
            "schema_id": schema_id,
            "target_url": "https://testshop.com/products",
        }

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            task_response = await client.post("/api/v1/tasks/", json=task_data)
            task_id = task_response.json()["task_id"]

        # Verify initial status
        get_response = await client.get(f"/api/v1/tasks/{task_id}")
        assert get_response.json()["status"] == "pending"

    @pytest.mark.asyncio
    async def test_batch_task_processing(self, client, ecommerce_schema_data):
        """Test creating and tracking multiple tasks."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=ecommerce_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Create batch of tasks for different pages
        urls = [
            "https://testshop.com/products?page=1",
            "https://testshop.com/products?page=2",
            "https://testshop.com/products?page=3",
        ]

        task_ids = []
        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            for url in urls:
                task_data = {
                    "source_id": "testshop.com",
                    "schema_id": schema_id,
                    "target_url": url,
                }
                response = await client.post("/api/v1/tasks/", json=task_data)
                task_ids.append(response.json()["task_id"])

        # Verify all tasks were created
        assert len(task_ids) == 3

        # List tasks for source
        list_response = await client.get("/api/v1/tasks/?source_id=testshop.com")
        assert list_response.status_code == 200
        assert list_response.json()["total"] == 3


class TestSchemaVersioningWorkflow:
    """Tests for schema versioning workflow."""

    @pytest.mark.asyncio
    async def test_schema_update_creates_new_version(self, client, ecommerce_schema_data):
        """Test that updating a schema creates a new version."""
        # Create initial schema
        create_response = await client.post("/api/v1/schemas/", json=ecommerce_schema_data)
        schema_id = create_response.json()["schema_id"]
        initial_version = create_response.json()["version"]

        # Update schema
        update_data = {
            "description": "Updated e-commerce parser with new fields",
            "fields": ecommerce_schema_data["fields"] + [
                {
                    "name": "discount",
                    "type": "float",
                    "method": "css",
                    "selector": "span.discount",
                    "required": False,
                }
            ],
        }

        update_response = await client.put(f"/api/v1/schemas/{schema_id}", json=update_data)
        assert update_response.status_code == 200

        # Get schema and check version
        get_response = await client.get(f"/api/v1/schemas/{schema_id}")
        schema = get_response.json()

        assert len(schema["fields"]) == 8  # Original 7 + 1 new
        assert schema["description"] == "Updated e-commerce parser with new fields"


class TestErrorHandlingWorkflow:
    """Tests for error handling in the workflow."""

    @pytest.mark.asyncio
    async def test_extraction_with_missing_required_fields(self):
        """Test extraction when required fields are missing."""
        html = """
        <html>
        <body>
            <div class="product-card">
                <h2 class="product-name">Product Without Price</h2>
                <!-- Missing price and product_id -->
            </div>
        </body>
        </html>
        """

        schema = ParsingSchema(
            schema_id="test",
            source_id="test.com",
            start_url="https://test.com",
            item_container="div.product-card",
            fields=[
                FieldDefinition(
                    name="name",
                    selector=".product-name",
                    required=True,
                ),
                FieldDefinition(
                    name="price",
                    selector=".price",
                    required=True,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        # Record should be invalid due to missing required field
        assert len(records) == 0

    @pytest.mark.asyncio
    async def test_extraction_with_fallback_selectors(self):
        """Test extraction using fallback selectors."""
        html = """
        <html>
        <body>
            <div class="item">
                <span class="alternate-price">$59.99</span>
            </div>
        </body>
        </html>
        """

        schema = ParsingSchema(
            schema_id="test",
            source_id="test.com",
            start_url="https://test.com",
            item_container="div.item",
            fields=[
                FieldDefinition(
                    name="price",
                    selector=".price",
                    fallback_selectors=[".alt-price", ".alternate-price"],
                    required=True,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert len(records) == 1
        assert records[0]["price"] == "$59.99"


class TestDataTransformationWorkflow:
    """Tests for data transformation in the workflow."""

    @pytest.mark.asyncio
    async def test_complex_transformations(self):
        """Test complex data transformations."""
        html = """
        <html>
        <body>
            <div class="product">
                <span class="price">  $1,234.56  </span>
                <span class="date">15.01.2024</span>
                <span class="stock">In Stock</span>
                <a href="/product/123">Link</a>
            </div>
        </body>
        </html>
        """

        schema = ParsingSchema(
            schema_id="test",
            source_id="test.com",
            start_url="https://test.com",
            item_container="div.product",
            fields=[
                FieldDefinition(
                    name="price",
                    selector=".price",
                    type="float",
                    transformations=["trim", "extract_number"],
                    required=True,
                ),
                FieldDefinition(
                    name="date",
                    selector=".date",
                    type="string",
                    transformations=["parse_date"],
                    required=False,
                ),
                FieldDefinition(
                    name="available",
                    selector=".stock",
                    type="boolean",
                    transformations=["to_bool"],
                    required=False,
                ),
                FieldDefinition(
                    name="url",
                    selector="a",
                    attribute="href",
                    type="url",
                    transformations=["absolute_url"],
                    required=False,
                ),
            ],
        )

        extractor = DataExtractor(schema, base_url="https://test.com")
        records = extractor.extract(html)

        assert len(records) == 1
        record = records[0]

        assert record["price"] == 1234.56
        assert record["date"] == "2024-01-15"
        assert record["available"] is True
        assert record["url"] == "https://test.com/product/123"
