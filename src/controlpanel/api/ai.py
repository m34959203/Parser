"""API endpoints for AI-powered schema generation."""

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from src.controlpanel.database import get_db

router = APIRouter(prefix="/ai", tags=["AI"])


class GenerationRequest(BaseModel):
    """Request for AI schema generation."""

    url: str = Field(..., description="URL to analyze")
    goal_description: str = Field(..., description="What data to extract")
    example_fields: list[str] | None = Field(None, description="Example field names")
    constraints: dict[str, Any] | None = Field(None, description="Additional constraints")


class GenerationTaskResponse(BaseModel):
    """Response for async generation task."""

    task_id: str
    status: str
    estimated_time_seconds: int


# In-memory cache for generation results (use Redis in production)
_generation_cache: dict[str, Any] = {}


@router.post("/generate", response_model=GenerationTaskResponse)
async def generate_schema(
    request: GenerationRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> GenerationTaskResponse:
    """Start AI-powered schema generation.

    Analyzes the target URL and generates a parsing schema based on
    the goal description. This is an async operation - use the task_id
    to poll for results.
    """
    task_id = str(uuid4())

    # Add background task
    background_tasks.add_task(
        _run_generation,
        task_id=task_id,
        request=request,
    )

    _generation_cache[task_id] = {"status": "processing"}

    return GenerationTaskResponse(
        task_id=task_id,
        status="processing",
        estimated_time_seconds=30,
    )


@router.get("/generate/{task_id}", response_model=dict[str, Any])
async def get_generation_result(task_id: str) -> dict[str, Any]:
    """Get the result of a schema generation task.

    Poll this endpoint until status is 'completed' or 'failed'.
    """
    result = _generation_cache.get(task_id)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Generation task '{task_id}' not found",
        )

    return result


@router.post("/analyze", response_model=dict[str, Any])
async def analyze_page(
    url: str,
    goal: str,
) -> dict[str, Any]:
    """Quick page analysis without full schema generation.

    Returns page structure, detected fields, and recommendations.
    """
    from src.ai_module.schema_generator import SchemaGenerator
    from src.config import get_settings

    settings = get_settings()

    try:
        generator = SchemaGenerator(settings.ai)
        page_data = await generator._crawl_page(url)
        structure = await generator._analyze_structure(page_data, goal)

        return {
            "url": url,
            "page_type": structure.page_type,
            "fields": [f.model_dump() for f in structure.fields],
            "pagination": structure.pagination.model_dump() if structure.pagination else None,
            "requires_js": structure.requires_js,
            "notes": structure.notes,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}",
        )


@router.post("/validate", response_model=dict[str, Any])
async def validate_schema_ai(
    schema: dict[str, Any],
    test_url: str,
) -> dict[str, Any]:
    """Validate a schema using AI-powered testing.

    Tests the schema against the URL and returns detailed results.
    """
    from src.ai_module.validator import SchemaValidator
    from src.shared.models import ParsingSchema

    try:
        parsing_schema = ParsingSchema(**schema)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid schema: {str(e)}",
        )

    validator = SchemaValidator()
    results = await validator.validate_schema(parsing_schema, test_url)

    return {
        "url": test_url,
        "success": results.success,
        "records_found": results.records_found,
        "field_results": results.field_results,
        "errors": results.errors,
        "suggestions": results.suggestions,
    }


@router.post("/improve", response_model=dict[str, Any])
async def improve_schema(
    schema: dict[str, Any],
    test_url: str,
    issues: list[str] | None = None,
) -> dict[str, Any]:
    """Use AI to improve a failing schema.

    Takes a schema and issues, returns an improved version.
    """
    from src.ai_module.schema_generator import SchemaGenerator
    from src.config import get_settings
    from src.shared.models import ParsingSchema

    settings = get_settings()

    try:
        parsing_schema = ParsingSchema(**schema)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid schema: {str(e)}",
        )

    generator = SchemaGenerator(settings.ai)

    try:
        improved = await generator.improve_schema(
            schema=parsing_schema,
            test_url=test_url,
            issues=issues or [],
        )

        return {
            "original_version": parsing_schema.version,
            "improved_schema": improved.model_dump(),
            "changes": improved.description,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Improvement failed: {str(e)}",
        )


async def _run_generation(task_id: str, request: GenerationRequest) -> None:
    """Background task for schema generation."""
    from src.ai_module.schema_generator import SchemaGenerator, GenerationRequest as GenReq
    from src.config import get_settings

    settings = get_settings()

    try:
        generator = SchemaGenerator(settings.ai)
        result = await generator.generate(
            GenReq(
                url=request.url,
                goal_description=request.goal_description,
                example_fields=request.example_fields,
                constraints=request.constraints,
            )
        )

        _generation_cache[task_id] = {
            "status": "completed",
            "schema": result.schema.model_dump(),
            "confidence": result.confidence,
            "warnings": result.warnings,
            "test_results": [r.model_dump() for r in result.test_results],
        }

    except Exception as e:
        _generation_cache[task_id] = {
            "status": "failed",
            "error": str(e),
        }
