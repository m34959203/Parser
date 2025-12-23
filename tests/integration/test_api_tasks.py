"""
Integration tests for Task API endpoints.
"""
import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock, MagicMock

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

from src.controlpanel.main import app
from src.controlpanel.database import Base, get_async_session


# Test database setup
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
def sample_schema_data():
    """Sample schema data for testing."""
    return {
        "source_id": "test.com",
        "start_url": "https://test.com/products",
        "fields": [
            {"name": "title", "selector": "h1.title"},
            {"name": "price", "selector": ".price"},
        ],
    }


@pytest.fixture
def sample_task_data():
    """Sample task data for testing."""
    return {
        "source_id": "test.com",
        "schema_id": "placeholder",  # Will be replaced with actual schema_id
        "target_url": "https://test.com/product/123",
        "mode": "http",
        "priority": 5,
        "max_attempts": 3,
        "metadata": {"test": True},
    }


class TestTaskCreateEndpoint:
    """Tests for POST /api/v1/tasks endpoint."""

    @pytest.mark.asyncio
    async def test_create_task_success(self, client, sample_schema_data, sample_task_data):
        """Test successful task creation."""
        # First create a schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        assert schema_response.status_code == 201
        schema_id = schema_response.json()["schema_id"]

        # Create task with the schema
        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        # Mock RabbitMQ client
        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            response = await client.post("/api/v1/tasks/", json=task_data)

        assert response.status_code == 201
        data = response.json()

        assert "task_id" in data
        assert data["source_id"] == task_data["source_id"]
        assert data["schema_id"] == schema_id
        assert data["target_url"] == task_data["target_url"]
        assert data["status"] == "pending"
        assert data["priority"] == 5

    @pytest.mark.asyncio
    async def test_create_task_browser_mode(self, client, sample_schema_data, sample_task_data):
        """Test creating a browser mode task."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Create browser task
        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id
        task_data["mode"] = "browser"

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            response = await client.post("/api/v1/tasks/", json=task_data)

        assert response.status_code == 201
        assert response.json()["mode"] == "browser"

    @pytest.mark.asyncio
    async def test_create_task_with_callback(self, client, sample_schema_data, sample_task_data):
        """Test creating task with callback URL."""
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id
        task_data["callback_url"] = "https://webhook.example.com/callback"

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            response = await client.post("/api/v1/tasks/", json=task_data)

        assert response.status_code == 201
        assert response.json()["callback_url"] == "https://webhook.example.com/callback"

    @pytest.mark.asyncio
    async def test_create_task_invalid_schema(self, client, sample_task_data):
        """Test creating task with non-existent schema."""
        task_data = sample_task_data.copy()
        task_data["schema_id"] = "nonexistent_schema"

        response = await client.post("/api/v1/tasks/", json=task_data)
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_create_task_missing_url(self, client, sample_schema_data):
        """Test that task creation fails without target_url."""
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        invalid_data = {
            "source_id": "test.com",
            "schema_id": schema_id,
            # Missing target_url
        }

        response = await client.post("/api/v1/tasks/", json=invalid_data)
        assert response.status_code == 422


class TestTaskGetEndpoint:
    """Tests for GET /api/v1/tasks/{task_id} endpoint."""

    @pytest.mark.asyncio
    async def test_get_task_success(self, client, sample_schema_data, sample_task_data):
        """Test getting an existing task."""
        # Create schema and task
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            create_response = await client.post("/api/v1/tasks/", json=task_data)

        task_id = create_response.json()["task_id"]

        # Get the task
        response = await client.get(f"/api/v1/tasks/{task_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == task_id
        assert data["source_id"] == task_data["source_id"]

    @pytest.mark.asyncio
    async def test_get_task_not_found(self, client):
        """Test getting a non-existent task."""
        response = await client.get("/api/v1/tasks/nonexistent_task_id")
        assert response.status_code == 404


class TestTaskListEndpoint:
    """Tests for GET /api/v1/tasks endpoint."""

    @pytest.mark.asyncio
    async def test_list_tasks_empty(self, client):
        """Test listing tasks when none exist."""
        response = await client.get("/api/v1/tasks/")

        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_tasks_with_data(self, client, sample_schema_data, sample_task_data):
        """Test listing tasks with data."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Create multiple tasks
        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            for i in range(3):
                task_data = sample_task_data.copy()
                task_data["schema_id"] = schema_id
                task_data["target_url"] = f"https://test.com/product/{i}"
                await client.post("/api/v1/tasks/", json=task_data)

        response = await client.get("/api/v1/tasks/")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
        assert data["total"] == 3

    @pytest.mark.asyncio
    async def test_list_tasks_pagination(self, client, sample_schema_data, sample_task_data):
        """Test task list pagination."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Create 5 tasks
        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            for i in range(5):
                task_data = sample_task_data.copy()
                task_data["schema_id"] = schema_id
                task_data["target_url"] = f"https://test.com/product/{i}"
                await client.post("/api/v1/tasks/", json=task_data)

        # Get first page
        response = await client.get("/api/v1/tasks/?page=1&page_size=2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5
        assert data["page"] == 1
        assert data["pages"] == 3

    @pytest.mark.asyncio
    async def test_list_tasks_filter_by_status(self, client, sample_schema_data, sample_task_data):
        """Test filtering tasks by status."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            await client.post("/api/v1/tasks/", json=task_data)

        response = await client.get("/api/v1/tasks/?status=pending")

        assert response.status_code == 200
        data = response.json()
        for item in data["items"]:
            assert item["status"] == "pending"

    @pytest.mark.asyncio
    async def test_list_tasks_filter_by_source(self, client, sample_schema_data, sample_task_data):
        """Test filtering tasks by source_id."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            await client.post("/api/v1/tasks/", json=task_data)

        response = await client.get("/api/v1/tasks/?source_id=test.com")

        assert response.status_code == 200
        data = response.json()
        for item in data["items"]:
            assert item["source_id"] == "test.com"


class TestTaskCancelEndpoint:
    """Tests for POST /api/v1/tasks/{task_id}/cancel endpoint."""

    @pytest.mark.asyncio
    async def test_cancel_pending_task(self, client, sample_schema_data, sample_task_data):
        """Test canceling a pending task."""
        # Create schema and task
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            create_response = await client.post("/api/v1/tasks/", json=task_data)

        task_id = create_response.json()["task_id"]

        # Cancel the task
        response = await client.post(f"/api/v1/tasks/{task_id}/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "cancelled" or data.get("cancelled") is True

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(self, client):
        """Test canceling a non-existent task."""
        response = await client.post("/api/v1/tasks/nonexistent/cancel")
        assert response.status_code == 404


class TestTaskRetryEndpoint:
    """Tests for POST /api/v1/tasks/{task_id}/retry endpoint."""

    @pytest.mark.asyncio
    async def test_retry_task(self, client, sample_schema_data, sample_task_data):
        """Test retrying a task."""
        # Create schema and task
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        task_data = sample_task_data.copy()
        task_data["schema_id"] = schema_id

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            create_response = await client.post("/api/v1/tasks/", json=task_data)
            task_id = create_response.json()["task_id"]

            # Retry the task
            response = await client.post(f"/api/v1/tasks/{task_id}/retry")

        # Should succeed or indicate it was requeued
        assert response.status_code in [200, 202]


class TestBatchTaskEndpoints:
    """Tests for batch task operations."""

    @pytest.mark.asyncio
    async def test_create_batch_tasks(self, client, sample_schema_data, sample_task_data):
        """Test creating multiple tasks in batch."""
        # Create schema
        schema_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = schema_response.json()["schema_id"]

        # Prepare batch data
        batch_data = {
            "tasks": [
                {
                    "source_id": "test.com",
                    "schema_id": schema_id,
                    "target_url": f"https://test.com/product/{i}",
                }
                for i in range(5)
            ]
        }

        with patch('src.controlpanel.services.task_service.RMQClient') as mock_rmq:
            mock_client = AsyncMock()
            mock_rmq.return_value = mock_client

            response = await client.post("/api/v1/tasks/batch", json=batch_data)

        # Should succeed and return created tasks
        assert response.status_code in [200, 201]
        data = response.json()
        assert "tasks" in data or "created" in data or isinstance(data, list)


class TestTaskStatsEndpoint:
    """Tests for task statistics endpoint."""

    @pytest.mark.asyncio
    async def test_get_task_stats(self, client):
        """Test getting task statistics."""
        response = await client.get("/api/v1/tasks/stats")

        assert response.status_code == 200
        data = response.json()

        # Should have status counts
        assert "total" in data or "pending" in data or "status_counts" in data
