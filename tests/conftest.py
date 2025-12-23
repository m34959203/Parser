"""
Pytest configuration and shared fixtures.
"""
import asyncio
import pytest
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

from src.controlpanel.database import Base
from src.shared.models.parsing_schema import ParsingSchema, FieldDefinition
from src.shared.models.task_message import TaskMessage, TaskStatus


# Configure asyncio for pytest
@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Database fixtures
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
async def async_session(async_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create an async session for testing."""
    async_session_maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session_maker() as session:
        yield session
        await session.rollback()


# Schema fixtures
@pytest.fixture
def sample_field_definition() -> dict:
    """Return a sample field definition as dict."""
    return {
        "name": "title",
        "selector": "h1.product-title",
        "selector_type": "css",
        "attribute": None,
        "default_value": None,
        "required": True,
        "multiple": False,
        "transformations": ["trim", "lowercase"],
        "nested_fields": None,
    }


@pytest.fixture
def sample_parsing_schema(sample_field_definition) -> dict:
    """Return a sample parsing schema as dict."""
    return {
        "id": "schema-001",
        "name": "Product Parser",
        "version": 1,
        "source_id": "ecommerce-site",
        "base_url": "https://example.com",
        "url_patterns": ["https://example.com/product/*"],
        "container_selector": "div.product-card",
        "fields": [sample_field_definition],
        "navigation": None,
        "pagination": None,
        "wait_for": None,
        "wait_timeout": 10000,
        "requires_javascript": False,
        "rate_limit_delay": 1.0,
        "headers": {"User-Agent": "TestBot/1.0"},
        "cookies": None,
        "metadata": {"category": "products"},
    }


@pytest.fixture
def sample_field_model() -> FieldDefinition:
    """Return a sample FieldDefinition Pydantic model."""
    return FieldDefinition(
        name="price",
        selector="span.price",
        selector_type="css",
        attribute="data-value",
        required=True,
        multiple=False,
        transformations=["extract_number"],
    )


@pytest.fixture
def sample_schema_model(sample_field_model) -> ParsingSchema:
    """Return a sample ParsingSchema Pydantic model."""
    return ParsingSchema(
        id="schema-002",
        name="Test Schema",
        version=1,
        source_id="test-source",
        base_url="https://test.com",
        url_patterns=["https://test.com/*"],
        fields=[sample_field_model],
        requires_javascript=True,
    )


# Task fixtures
@pytest.fixture
def sample_task_message() -> dict:
    """Return a sample task message as dict."""
    return {
        "task_id": "task-001",
        "source_id": "test-source",
        "schema_id": "schema-001",
        "target_url": "https://example.com/product/123",
        "mode": "http",
        "priority": 5,
        "attempt": 1,
        "max_attempts": 3,
        "callback_url": None,
        "metadata": {"test": True},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "scheduled_for": None,
    }


@pytest.fixture
def sample_task_model(sample_task_message) -> TaskMessage:
    """Return a sample TaskMessage Pydantic model."""
    return TaskMessage(**sample_task_message)


# Mock fixtures
@pytest.fixture
def mock_rmq_client() -> MagicMock:
    """Return a mocked RabbitMQ client."""
    client = MagicMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.publish_task = AsyncMock()
    client.consume_tasks = AsyncMock()
    client.publish_result = AsyncMock()
    return client


@pytest.fixture
def mock_delta_client() -> MagicMock:
    """Return a mocked Delta Lake client."""
    client = MagicMock()
    client.write_bronze = AsyncMock()
    client.write_silver = AsyncMock()
    client.read_bronze = AsyncMock(return_value=[])
    client.read_silver = AsyncMock(return_value=[])
    return client


@pytest.fixture
def mock_http_session() -> MagicMock:
    """Return a mocked aiohttp session."""
    session = MagicMock()
    response = MagicMock()
    response.status = 200
    response.text = AsyncMock(return_value="<html><body><h1>Test</h1></body></html>")
    response.headers = {"content-type": "text/html"}

    session.get = AsyncMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock()

    return session


# HTML fixtures for extraction tests
@pytest.fixture
def sample_html() -> str:
    """Return sample HTML for testing extractors."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Test Page</title></head>
    <body>
        <div class="product-list">
            <div class="product-card" data-id="1">
                <h2 class="title">Product One</h2>
                <span class="price" data-value="29.99">$29.99</span>
                <p class="description">First product description</p>
                <a href="/product/1" class="link">View</a>
            </div>
            <div class="product-card" data-id="2">
                <h2 class="title">Product Two</h2>
                <span class="price" data-value="49.99">$49.99</span>
                <p class="description">Second product description</p>
                <a href="/product/2" class="link">View</a>
            </div>
            <div class="product-card" data-id="3">
                <h2 class="title">  Product Three  </h2>
                <span class="price" data-value="99.99">$99.99</span>
                <p class="description">Third product description</p>
                <a href="/product/3" class="link">View</a>
            </div>
        </div>
        <div class="pagination">
            <a href="/page/2" class="next">Next</a>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_json_data() -> list:
    """Return sample JSON data for testing."""
    return [
        {"id": 1, "name": "Item 1", "price": 10.0, "category": "A"},
        {"id": 2, "name": "Item 2", "price": 20.0, "category": "B"},
        {"id": 3, "name": "Item 3", "price": 30.0, "category": "A"},
    ]
