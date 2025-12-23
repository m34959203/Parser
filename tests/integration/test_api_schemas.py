"""
Integration tests for Schema API endpoints.
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
async def async_session(async_engine) -> AsyncSession:
    """Create an async session for testing."""
    async_session_maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session_maker() as session:
        yield session


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
        "description": "Test schema for integration tests",
        "start_url": "https://test.com/products",
        "url_pattern": r"https://test\.com/products/.*",
        "item_container": "div.product-card",
        "fields": [
            {
                "name": "title",
                "type": "string",
                "method": "css",
                "selector": "h2.title",
                "required": True,
                "transformations": ["trim"],
            },
            {
                "name": "price",
                "type": "float",
                "method": "css",
                "selector": "span.price",
                "required": True,
                "transformations": ["extract_number"],
            },
            {
                "name": "url",
                "type": "url",
                "method": "css",
                "selector": "a.product-link",
                "attribute": "href",
                "required": False,
            },
        ],
        "min_fields_required": 2,
        "dedup_keys": ["title", "url"],
        "mode": "http",
        "requires_js": False,
        "tags": ["products", "test"],
    }


class TestSchemaCreateEndpoint:
    """Tests for POST /api/v1/schemas endpoint."""

    @pytest.mark.asyncio
    async def test_create_schema_success(self, client, sample_schema_data):
        """Test successful schema creation."""
        response = await client.post("/api/v1/schemas/", json=sample_schema_data)

        assert response.status_code == 201
        data = response.json()

        assert "schema_id" in data
        assert data["source_id"] == sample_schema_data["source_id"]
        assert data["description"] == sample_schema_data["description"]
        assert data["start_url"] == sample_schema_data["start_url"]
        assert len(data["fields"]) == 3
        assert data["is_active"] is True

    @pytest.mark.asyncio
    async def test_create_schema_minimal(self, client):
        """Test creating schema with minimal required fields."""
        minimal_data = {
            "source_id": "minimal.com",
            "start_url": "https://minimal.com",
            "fields": [
                {"name": "data", "selector": ".data"},
            ],
        }

        response = await client.post("/api/v1/schemas/", json=minimal_data)

        assert response.status_code == 201
        data = response.json()
        assert data["source_id"] == "minimal.com"
        assert len(data["fields"]) == 1

    @pytest.mark.asyncio
    async def test_create_schema_invalid_no_fields(self, client):
        """Test that schema creation fails without fields."""
        invalid_data = {
            "source_id": "test.com",
            "start_url": "https://test.com",
            "fields": [],
        }

        response = await client.post("/api/v1/schemas/", json=invalid_data)
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_schema_duplicate_field_names(self, client):
        """Test that schema creation fails with duplicate field names."""
        invalid_data = {
            "source_id": "test.com",
            "start_url": "https://test.com",
            "fields": [
                {"name": "title", "selector": ".title"},
                {"name": "title", "selector": ".other"},
            ],
        }

        response = await client.post("/api/v1/schemas/", json=invalid_data)
        assert response.status_code == 422


class TestSchemaGetEndpoint:
    """Tests for GET /api/v1/schemas/{schema_id} endpoint."""

    @pytest.mark.asyncio
    async def test_get_schema_success(self, client, sample_schema_data):
        """Test getting an existing schema."""
        # First create a schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        assert create_response.status_code == 201
        schema_id = create_response.json()["schema_id"]

        # Then get it
        response = await client.get(f"/api/v1/schemas/{schema_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["schema_id"] == schema_id
        assert data["source_id"] == sample_schema_data["source_id"]

    @pytest.mark.asyncio
    async def test_get_schema_not_found(self, client):
        """Test getting a non-existent schema."""
        response = await client.get("/api/v1/schemas/nonexistent_schema_id")
        assert response.status_code == 404


class TestSchemaListEndpoint:
    """Tests for GET /api/v1/schemas endpoint."""

    @pytest.mark.asyncio
    async def test_list_schemas_empty(self, client):
        """Test listing schemas when none exist."""
        response = await client.get("/api/v1/schemas/")

        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_schemas_with_data(self, client, sample_schema_data):
        """Test listing schemas with data."""
        # Create multiple schemas
        for i in range(3):
            schema_data = sample_schema_data.copy()
            schema_data["source_id"] = f"test{i}.com"
            await client.post("/api/v1/schemas/", json=schema_data)

        response = await client.get("/api/v1/schemas/")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
        assert data["total"] == 3

    @pytest.mark.asyncio
    async def test_list_schemas_pagination(self, client, sample_schema_data):
        """Test schema list pagination."""
        # Create 5 schemas
        for i in range(5):
            schema_data = sample_schema_data.copy()
            schema_data["source_id"] = f"test{i}.com"
            await client.post("/api/v1/schemas/", json=schema_data)

        # Get first page
        response = await client.get("/api/v1/schemas/?page=1&page_size=2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
        assert data["total"] == 5
        assert data["page"] == 1
        assert data["pages"] == 3

    @pytest.mark.asyncio
    async def test_list_schemas_filter_by_source(self, client, sample_schema_data):
        """Test filtering schemas by source_id."""
        # Create schemas with different sources
        for source in ["alpha.com", "beta.com", "alpha.com"]:
            schema_data = sample_schema_data.copy()
            schema_data["source_id"] = source
            await client.post("/api/v1/schemas/", json=schema_data)

        response = await client.get("/api/v1/schemas/?source_id=alpha.com")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
        for item in data["items"]:
            assert item["source_id"] == "alpha.com"

    @pytest.mark.asyncio
    async def test_list_schemas_filter_by_active(self, client, sample_schema_data):
        """Test filtering schemas by active status."""
        # Create active schema
        await client.post("/api/v1/schemas/", json=sample_schema_data)

        response = await client.get("/api/v1/schemas/?is_active=true")

        assert response.status_code == 200
        data = response.json()
        for item in data["items"]:
            assert item["is_active"] is True


class TestSchemaUpdateEndpoint:
    """Tests for PUT /api/v1/schemas/{schema_id} endpoint."""

    @pytest.mark.asyncio
    async def test_update_schema_success(self, client, sample_schema_data):
        """Test successful schema update."""
        # Create schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = create_response.json()["schema_id"]

        # Update it
        update_data = {
            "description": "Updated description",
            "is_active": False,
        }

        response = await client.put(f"/api/v1/schemas/{schema_id}", json=update_data)

        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Updated description"
        assert data["is_active"] is False

    @pytest.mark.asyncio
    async def test_update_schema_fields(self, client, sample_schema_data):
        """Test updating schema fields."""
        # Create schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = create_response.json()["schema_id"]

        # Update fields
        update_data = {
            "fields": [
                {"name": "new_field", "selector": ".new", "method": "css"},
            ],
        }

        response = await client.put(f"/api/v1/schemas/{schema_id}", json=update_data)

        assert response.status_code == 200
        data = response.json()
        assert len(data["fields"]) == 1
        assert data["fields"][0]["name"] == "new_field"

    @pytest.mark.asyncio
    async def test_update_schema_not_found(self, client):
        """Test updating non-existent schema."""
        update_data = {"description": "Updated"}

        response = await client.put("/api/v1/schemas/nonexistent", json=update_data)
        assert response.status_code == 404


class TestSchemaDeleteEndpoint:
    """Tests for DELETE /api/v1/schemas/{schema_id} endpoint."""

    @pytest.mark.asyncio
    async def test_delete_schema_success(self, client, sample_schema_data):
        """Test successful schema deletion."""
        # Create schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = create_response.json()["schema_id"]

        # Delete it
        response = await client.delete(f"/api/v1/schemas/{schema_id}")
        assert response.status_code == 204

        # Verify it's gone
        get_response = await client.get(f"/api/v1/schemas/{schema_id}")
        assert get_response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_schema_not_found(self, client):
        """Test deleting non-existent schema."""
        response = await client.delete("/api/v1/schemas/nonexistent")
        assert response.status_code == 404


class TestSchemaValidateEndpoint:
    """Tests for POST /api/v1/schemas/{schema_id}/validate endpoint."""

    @pytest.mark.asyncio
    async def test_validate_schema_success(self, client, sample_schema_data):
        """Test schema validation endpoint."""
        # Create schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = create_response.json()["schema_id"]

        # Validate with test HTML
        test_html = """
        <html>
        <body>
            <div class="product-card">
                <h2 class="title">Test Product</h2>
                <span class="price">$29.99</span>
            </div>
        </body>
        </html>
        """

        response = await client.post(
            f"/api/v1/schemas/{schema_id}/validate",
            json={"html": test_html}
        )

        assert response.status_code == 200
        data = response.json()
        assert "records" in data or "valid" in data


class TestSchemaVersionEndpoint:
    """Tests for schema versioning endpoints."""

    @pytest.mark.asyncio
    async def test_get_schema_versions(self, client, sample_schema_data):
        """Test getting schema version history."""
        # Create schema
        create_response = await client.post("/api/v1/schemas/", json=sample_schema_data)
        schema_id = create_response.json()["schema_id"]

        # Get versions
        response = await client.get(f"/api/v1/schemas/{schema_id}/versions")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1  # At least the initial version
