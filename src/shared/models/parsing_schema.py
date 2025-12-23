"""Parsing schema models - core data extraction configuration."""

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class FieldType(str, Enum):
    """Supported field data types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    URL = "url"
    LIST = "list"
    JSON = "json"


class ExtractionMethod(str, Enum):
    """Data extraction methods."""

    CSS = "css"
    XPATH = "xpath"
    REGEX = "regex"
    JSON_PATH = "json_path"


class FieldDefinition(BaseModel):
    """Definition of a single field to extract."""

    name: str = Field(..., min_length=1, max_length=100, description="Field name")
    type: FieldType = Field(default=FieldType.STRING, description="Data type")
    method: ExtractionMethod = Field(default=ExtractionMethod.CSS, description="Extraction method")
    selector: str = Field(..., min_length=1, description="Selector expression")
    attribute: str | None = Field(default=None, description="HTML attribute to extract (e.g., 'href', 'src')")
    required: bool = Field(default=True, description="Whether field is required")
    default: str | None = Field(default=None, description="Default value if not found")
    transformations: list[str] = Field(
        default_factory=list,
        description="List of transformations: trim, lowercase, uppercase, extract_number, etc."
    )
    validation_regex: str | None = Field(default=None, description="Regex pattern for validation")
    fallback_selectors: list[str] = Field(
        default_factory=list,
        description="Alternative selectors if primary fails"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "price",
                "type": "float",
                "method": "css",
                "selector": "span.price",
                "transformations": ["trim", "extract_number"],
                "required": True,
            }
        }
    }


class NavigationStep(BaseModel):
    """Single navigation step in a multi-step scenario."""

    action: Literal["goto", "click", "scroll", "wait", "input", "screenshot", "hover", "select"] = Field(
        ..., description="Action to perform"
    )
    target: str | None = Field(default=None, description="CSS selector or URL for the action")
    value: str | None = Field(default=None, description="Value for input actions")
    wait_ms: int = Field(default=0, ge=0, le=60000, description="Wait time after action (ms)")
    wait_for: str | None = Field(default=None, description="CSS selector to wait for")
    optional: bool = Field(default=False, description="If true, step failure won't stop execution")

    model_config = {
        "json_schema_extra": {
            "example": {
                "action": "click",
                "target": "button.load-more",
                "wait_ms": 1000,
                "wait_for": "div.products-loaded",
            }
        }
    }


class PaginationRule(BaseModel):
    """Pagination configuration for multi-page scraping."""

    type: Literal["next_button", "page_param", "infinite_scroll", "load_more", "none"] = Field(
        ..., description="Pagination type"
    )
    selector: str | None = Field(default=None, description="Selector for next button/load more")
    param_name: str | None = Field(default=None, description="URL parameter name for page number")
    param_start: int = Field(default=1, description="Starting page number")
    param_step: int = Field(default=1, description="Page number increment")
    max_pages: int = Field(default=10, ge=1, le=1000, description="Maximum pages to scrape")
    stop_selector: str | None = Field(default=None, description="Selector indicating last page")
    scroll_delay_ms: int = Field(default=1000, description="Delay between scrolls for infinite scroll")


class ParsingSchema(BaseModel):
    """Complete parsing schema definition."""

    schema_id: str = Field(..., min_length=1, max_length=100, description="Unique schema identifier")
    version: str = Field(default="1.0.0", pattern=r"^\d+\.\d+\.\d+$", description="Semantic version")
    source_id: str = Field(..., min_length=1, description="Source identifier (domain/section)")
    description: str = Field(default="", max_length=1000, description="Schema description")

    # Target configuration
    start_url: str = Field(..., description="Starting URL for scraping")
    url_pattern: str | None = Field(default=None, description="URL pattern for matching (regex)")

    # Navigation
    navigation_steps: list[NavigationStep] = Field(
        default_factory=list, description="Pre-extraction navigation steps"
    )
    pagination: PaginationRule | None = Field(default=None, description="Pagination configuration")

    # Extraction
    item_container: str | None = Field(
        default=None, description="CSS selector for repeating item container"
    )
    fields: list[FieldDefinition] = Field(..., min_length=1, description="Fields to extract")

    # Quality rules
    min_fields_required: int = Field(default=1, ge=1, description="Minimum required fields per record")
    dedup_keys: list[str] = Field(default_factory=list, description="Fields for deduplication")

    # Execution settings
    mode: Literal["http", "browser"] = Field(default="http", description="Execution mode")
    requires_js: bool = Field(default=False, description="Whether JavaScript rendering is required")
    request_headers: dict[str, str] = Field(default_factory=dict, description="Custom HTTP headers")

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str | None = Field(default=None, description="Creator user ID")
    confidence: float | None = Field(default=None, ge=0, le=1, description="AI confidence score")
    is_active: bool = Field(default=True, description="Whether schema is active")
    tags: list[str] = Field(default_factory=list, description="Schema tags for categorization")

    @field_validator("fields")
    @classmethod
    def validate_unique_field_names(cls, v: list[FieldDefinition]) -> list[FieldDefinition]:
        """Ensure all field names are unique."""
        names = [f.name for f in v]
        if len(names) != len(set(names)):
            raise ValueError("Field names must be unique")
        return v

    @field_validator("dedup_keys")
    @classmethod
    def validate_dedup_keys(cls, v: list[str], info) -> list[str]:
        """Ensure dedup keys reference existing fields."""
        if info.data.get("fields"):
            field_names = {f.name for f in info.data["fields"]}
            for key in v:
                if key not in field_names:
                    raise ValueError(f"Dedup key '{key}' not found in fields")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "schema_id": "example_shop_products",
                "version": "1.0.0",
                "source_id": "example.com/products",
                "description": "Product catalog parser for example.com",
                "start_url": "https://example.com/catalog",
                "item_container": "div.product-card",
                "fields": [
                    {
                        "name": "title",
                        "type": "string",
                        "method": "css",
                        "selector": "h2.product-title",
                        "transformations": ["trim"],
                    },
                    {
                        "name": "price",
                        "type": "float",
                        "method": "css",
                        "selector": "span.price",
                        "transformations": ["extract_number"],
                    },
                    {
                        "name": "url",
                        "type": "url",
                        "method": "css",
                        "selector": "a.product-link",
                        "attribute": "href",
                    },
                ],
                "pagination": {
                    "type": "next_button",
                    "selector": "a.next-page",
                    "max_pages": 50,
                },
                "dedup_keys": ["title", "url"],
                "mode": "http",
            }
        }
    }


class ParsingSchemaCreate(BaseModel):
    """Schema for creating a new parsing schema."""

    source_id: str = Field(..., min_length=1)
    description: str = Field(default="")
    start_url: str
    url_pattern: str | None = None
    navigation_steps: list[NavigationStep] = Field(default_factory=list)
    pagination: PaginationRule | None = None
    item_container: str | None = None
    fields: list[FieldDefinition]
    min_fields_required: int = 1
    dedup_keys: list[str] = Field(default_factory=list)
    mode: Literal["http", "browser"] = "http"
    requires_js: bool = False
    request_headers: dict[str, str] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class ParsingSchemaUpdate(BaseModel):
    """Schema for updating an existing parsing schema."""

    description: str | None = None
    start_url: str | None = None
    url_pattern: str | None = None
    navigation_steps: list[NavigationStep] | None = None
    pagination: PaginationRule | None = None
    item_container: str | None = None
    fields: list[FieldDefinition] | None = None
    min_fields_required: int | None = None
    dedup_keys: list[str] | None = None
    mode: Literal["http", "browser"] | None = None
    requires_js: bool | None = None
    request_headers: dict[str, str] | None = None
    is_active: bool | None = None
    tags: list[str] | None = None
