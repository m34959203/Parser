"""API endpoints for statistics and monitoring."""

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.controlpanel.database import get_db
from src.controlpanel.services import TaskService
from src.shared.rmq_client import get_rmq_client

router = APIRouter(prefix="/stats", tags=["Statistics"])


@router.get("/overview", response_model=dict[str, Any])
async def get_overview_stats(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get overview statistics for the dashboard.

    Returns task counts by status, success rates, and queue statistics.
    """
    task_service = TaskService(db)
    task_stats = await task_service.get_stats()

    # Get queue stats
    rmq_client = await get_rmq_client()
    queue_stats = {}

    for queue_name in ["tasks.http", "tasks.browser", "results", "dlq.tasks"]:
        try:
            stats = await rmq_client.get_queue_stats(queue_name)
            queue_stats[queue_name] = stats
        except Exception:
            queue_stats[queue_name] = {"message_count": 0, "consumer_count": 0}

    return {
        "tasks": task_stats,
        "queues": queue_stats,
    }


@router.get("/tasks", response_model=dict[str, Any])
async def get_task_stats(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Get detailed task statistics.

    Returns counts by status, source, and time periods.
    """
    task_service = TaskService(db)
    return await task_service.get_stats()


@router.get("/queues", response_model=dict[str, Any])
async def get_queue_stats() -> dict[str, Any]:
    """Get RabbitMQ queue statistics.

    Returns message counts and consumer counts for all queues.
    """
    rmq_client = await get_rmq_client()
    stats = {}

    for queue_name in ["tasks.http", "tasks.browser", "results", "dlq.tasks"]:
        try:
            queue_stats = await rmq_client.get_queue_stats(queue_name)
            stats[queue_name] = queue_stats
        except Exception as e:
            stats[queue_name] = {"error": str(e)}

    return stats


@router.get("/health", response_model=dict[str, Any])
async def health_check(
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Health check endpoint.

    Checks connectivity to database and message queue.
    """
    health = {
        "status": "healthy",
        "components": {},
    }

    # Check database
    try:
        from sqlalchemy import text
        await db.execute(text("SELECT 1"))
        health["components"]["database"] = {"status": "healthy"}
    except Exception as e:
        health["status"] = "unhealthy"
        health["components"]["database"] = {"status": "unhealthy", "error": str(e)}

    # Check RabbitMQ
    try:
        rmq_client = await get_rmq_client()
        await rmq_client.get_queue_stats("tasks.http")
        health["components"]["rabbitmq"] = {"status": "healthy"}
    except Exception as e:
        health["status"] = "unhealthy"
        health["components"]["rabbitmq"] = {"status": "unhealthy", "error": str(e)}

    return health
