"""Shared Pydantic models for messaging and data contracts."""

from .parsing_schema import (
    ExtractionMethod,
    FieldDefinition,
    FieldType,
    NavigationStep,
    PaginationRule,
    ParsingSchema,
    ParsingSchemaCreate,
    ParsingSchemaUpdate,
)
from .result_message import DataPointers, ErrorDetail, ExecutionMetrics, ResultMessage
from .task_message import TaskCreate, TaskMessage, TaskStatus

__all__ = [
    # Parsing Schema
    "FieldType",
    "ExtractionMethod",
    "FieldDefinition",
    "NavigationStep",
    "PaginationRule",
    "ParsingSchema",
    "ParsingSchemaCreate",
    "ParsingSchemaUpdate",
    # Task
    "TaskMessage",
    "TaskCreate",
    "TaskStatus",
    # Result
    "ResultMessage",
    "ExecutionMetrics",
    "DataPointers",
    "ErrorDetail",
]
