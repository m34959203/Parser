"""API endpoints for parsing schemas."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.controlpanel.database import get_db
from src.controlpanel.services import SchemaService
from src.shared.models import ParsingSchema, ParsingSchemaCreate, ParsingSchemaUpdate

router = APIRouter(prefix="/schemas", tags=["Schemas"])


@router.post("/", response_model=ParsingSchema, status_code=201)
async def create_schema(
    schema: ParsingSchemaCreate,
    db: AsyncSession = Depends(get_db),
) -> ParsingSchema:
    """Create a new parsing schema.

    Creates a new schema for extracting data from web pages.
    The schema defines what fields to extract and how to extract them.
    """
    service = SchemaService(db)
    return await service.create(schema)


@router.get("/", response_model=dict[str, Any])
async def list_schemas(
    source_id: str | None = Query(None, description="Filter by source ID"),
    is_active: bool | None = Query(None, description="Filter by active status"),
    tags: list[str] | None = Query(None, description="Filter by tags"),
    limit: int = Query(50, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """List parsing schemas with optional filters.

    Returns a paginated list of schemas with total count.
    """
    service = SchemaService(db)
    schemas, total = await service.list(
        source_id=source_id,
        is_active=is_active,
        tags=tags,
        limit=limit,
        offset=offset,
    )
    return {
        "items": [s.model_dump() for s in schemas],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{schema_id}", response_model=ParsingSchema)
async def get_schema(
    schema_id: str,
    version: str | None = Query(None, description="Specific version to retrieve"),
    db: AsyncSession = Depends(get_db),
) -> ParsingSchema:
    """Get a parsing schema by ID.

    Optionally retrieve a specific version of the schema.
    """
    service = SchemaService(db)
    schema = await service.get(schema_id, version)

    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_id}' not found" + (f" version {version}" if version else ""),
        )

    return schema


@router.get("/{schema_id}/versions", response_model=list[dict[str, Any]])
async def list_schema_versions(
    schema_id: str,
    db: AsyncSession = Depends(get_db),
) -> list[dict[str, Any]]:
    """List all versions of a schema.

    Returns version history with change descriptions and timestamps.
    """
    service = SchemaService(db)
    versions = await service.list_versions(schema_id)

    if not versions:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_id}' not found",
        )

    return versions


@router.put("/{schema_id}", response_model=ParsingSchema)
async def update_schema(
    schema_id: str,
    schema: ParsingSchemaUpdate,
    db: AsyncSession = Depends(get_db),
) -> ParsingSchema:
    """Update a parsing schema.

    Creates a new version with the updates.
    Previous versions are preserved in version history.
    """
    service = SchemaService(db)
    updated = await service.update(schema_id, schema)

    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_id}' not found",
        )

    return updated


@router.delete("/{schema_id}", status_code=204)
async def delete_schema(
    schema_id: str,
    version: str | None = Query(None, description="Specific version to delete"),
    db: AsyncSession = Depends(get_db),
) -> None:
    """Delete a schema or specific version.

    If version is specified, only that version is deleted.
    Otherwise, the entire schema with all versions is deleted.
    """
    service = SchemaService(db)
    deleted = await service.delete(schema_id, version)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_id}' not found" + (f" version {version}" if version else ""),
        )


@router.post("/{schema_id}/validate", response_model=dict[str, Any])
async def validate_schema(
    schema_id: str,
    test_urls: list[str],
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Test a schema on specified URLs.

    Runs the schema against the provided URLs and returns extraction results.
    This is a dry-run that doesn't save data to the data lake.
    """
    service = SchemaService(db)
    schema = await service.get(schema_id)

    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_id}' not found",
        )

    # Import validator here to avoid circular imports
    from src.ai_module.validator import SchemaValidator

    validator = SchemaValidator()
    results = []

    for url in test_urls[:5]:  # Limit to 5 URLs
        result = await validator.validate_schema(schema, url)
        results.append({
            "url": url,
            "success": result.success,
            "records_found": result.records_found,
            "fields_extracted": result.fields_extracted,
            "errors": result.errors,
        })

    return {
        "schema_id": schema_id,
        "version": schema.version,
        "test_results": results,
        "overall_success": all(r["success"] for r in results),
    }


@router.post("/{schema_id}/activate", status_code=200)
async def activate_schema(
    schema_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    """Activate a schema for use.

    Only active schemas can be used for task creation.
    """
    service = SchemaService(db)
    updated = await service.update(
        schema_id,
        ParsingSchemaUpdate(is_active=True),
    )

    if not updated:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_id}' not found")

    return {"status": "activated", "schema_id": schema_id}


@router.post("/{schema_id}/deactivate", status_code=200)
async def deactivate_schema(
    schema_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    """Deactivate a schema.

    Deactivated schemas cannot be used for new tasks.
    """
    service = SchemaService(db)
    updated = await service.update(
        schema_id,
        ParsingSchemaUpdate(is_active=False),
    )

    if not updated:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_id}' not found")

    return {"status": "deactivated", "schema_id": schema_id}
