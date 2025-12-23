"""API endpoints for parsing tasks."""

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.controlpanel.database import get_db
from src.controlpanel.services import TaskService
from src.shared.models import TaskCreate, TaskDetail, TaskListResponse, TaskResponse, TaskStatus

router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.post("/", response_model=TaskResponse, status_code=201)
async def create_task(
    task: TaskCreate,
    db: AsyncSession = Depends(get_db),
) -> TaskResponse:
    """Create a new parsing task.

    Creates a task and publishes it to the message queue for processing.
    The task will be picked up by an available worker based on the mode (http/browser).
    """
    service = TaskService(db)
    db_task, task_message = await service.create(task)

    return TaskResponse(
        task_id=db_task.id,
        status=db_task.status,
        message=f"Task created and queued for {task.mode} processing",
        created_at=db_task.created_at,
    )


@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    status: TaskStatus | None = Query(None, description="Filter by status"),
    source_id: str | None = Query(None, description="Filter by source ID"),
    schema_id: str | None = Query(None, description="Filter by schema ID"),
    limit: int = Query(50, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: AsyncSession = Depends(get_db),
) -> TaskListResponse:
    """List tasks with optional filters.

    Returns a paginated list of tasks with total count.
    """
    service = TaskService(db)
    tasks, total = await service.list(
        status=status,
        source_id=source_id,
        schema_id=schema_id,
        limit=limit,
        offset=offset,
    )

    return TaskListResponse(
        items=tasks,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/dlq", response_model=list[TaskDetail])
async def list_dlq_tasks(
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> list[TaskDetail]:
    """List tasks in Dead Letter Queue.

    Returns tasks that have failed all retry attempts.
    These tasks need manual intervention.
    """
    service = TaskService(db)
    return await service.get_dlq_tasks(limit)


@router.get("/{task_id}", response_model=TaskDetail)
async def get_task(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> TaskDetail:
    """Get task details by ID.

    Returns detailed information about a specific task including
    its current status, execution metrics, and any errors.
    """
    service = TaskService(db)
    task = await service.get(task_id)

    if not task:
        raise HTTPException(
            status_code=404,
            detail=f"Task '{task_id}' not found",
        )

    return task


@router.post("/{task_id}/retry", response_model=dict[str, Any])
async def retry_task(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retry a failed task.

    Resets the task's attempt counter and republishes it to the queue.
    Only failed or DLQ tasks can be retried.
    """
    service = TaskService(db)
    success = await service.retry(task_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry task '{task_id}'. Task not found or not in failed/dlq status.",
        )

    return {
        "status": "retried",
        "task_id": str(task_id),
        "message": "Task has been requeued for processing",
    }


@router.post("/{task_id}/cancel", response_model=dict[str, Any])
async def cancel_task(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Cancel a pending or queued task.

    Only tasks that haven't started processing can be cancelled.
    """
    service = TaskService(db)
    success = await service.cancel(task_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel task '{task_id}'. Task not found or already processing.",
        )

    return {
        "status": "cancelled",
        "task_id": str(task_id),
        "message": "Task has been cancelled",
    }


@router.post("/dlq/{task_id}/requeue", response_model=dict[str, Any])
async def requeue_dlq_task(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Move a task from DLQ back to the main queue.

    This is an alias for retry, specifically for DLQ tasks.
    """
    service = TaskService(db)
    success = await service.retry(task_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot requeue task '{task_id}'. Task not found or not in DLQ.",
        )

    return {
        "status": "requeued",
        "task_id": str(task_id),
        "message": "Task has been moved from DLQ to main queue",
    }


@router.post("/batch", response_model=list[TaskResponse], status_code=201)
async def create_batch_tasks(
    tasks: list[TaskCreate],
    db: AsyncSession = Depends(get_db),
) -> list[TaskResponse]:
    """Create multiple tasks in a batch.

    Limited to 100 tasks per request.
    All tasks are created and queued atomically.
    """
    if len(tasks) > 100:
        raise HTTPException(
            status_code=400,
            detail="Maximum 100 tasks per batch",
        )

    service = TaskService(db)
    responses = []

    for task in tasks:
        db_task, _ = await service.create(task)
        responses.append(
            TaskResponse(
                task_id=db_task.id,
                status=db_task.status,
                message="Task created",
                created_at=db_task.created_at,
            )
        )

    return responses
