"""Service for managing parsing tasks."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from src.controlpanel.models import TaskModel, TaskRunModel
from src.shared.models import TaskCreate, TaskDetail, TaskMessage, TaskStatus
from src.shared.rmq_client import get_rmq_client

logger = structlog.get_logger()


class TaskService:
    """Service for CRUD operations on tasks."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, task_data: TaskCreate) -> tuple[TaskModel, TaskMessage]:
        """Create a new task and publish to queue."""
        # Create database model
        db_task = TaskModel(
            source_id=task_data.source_id,
            target_url=task_data.target_url,
            schema_id=task_data.schema_id,
            schema_version=task_data.schema_version,
            mode=task_data.mode,
            status=TaskStatus.PENDING,
            priority=task_data.priority,
            max_attempts=task_data.max_attempts,
            proxy_profile_id=task_data.proxy_profile_id,
            session_profile_id=task_data.session_profile_id,
            context=task_data.context,
            scheduled_at=task_data.scheduled_at,
        )

        self.db.add(db_task)
        await self.db.flush()

        # Create task message for RabbitMQ
        task_message = TaskMessage(
            task_id=db_task.id,
            source_id=task_data.source_id,
            target_url=task_data.target_url,
            mode=task_data.mode,
            schema_id=task_data.schema_id,
            schema_version=task_data.schema_version,
            priority=task_data.priority,
            max_attempts=task_data.max_attempts,
            proxy_profile_id=task_data.proxy_profile_id,
            session_profile_id=task_data.session_profile_id,
            context=task_data.context,
            scheduled_at=task_data.scheduled_at,
            max_pages=task_data.max_pages,
        )

        # Publish to queue (if not scheduled for later)
        if not task_data.scheduled_at or task_data.scheduled_at <= datetime.utcnow():
            await self._publish_task(task_message)
            db_task.status = TaskStatus.QUEUED

        await self.db.commit()
        await self.db.refresh(db_task)

        logger.info(
            "Created task",
            task_id=str(db_task.id),
            source_id=task_data.source_id,
            target_url=task_data.target_url,
        )

        return db_task, task_message

    async def get(self, task_id: UUID) -> TaskDetail | None:
        """Get task details by ID."""
        stmt = select(TaskModel).where(TaskModel.id == task_id)
        result = await self.db.execute(stmt)
        db_task = result.scalar_one_or_none()

        if not db_task:
            return None

        # Get latest run
        run_stmt = select(TaskRunModel).where(
            TaskRunModel.task_id == task_id
        ).order_by(TaskRunModel.created_at.desc()).limit(1)
        run_result = await self.db.execute(run_stmt)
        latest_run = run_result.scalar_one_or_none()

        return TaskDetail(
            task_id=db_task.id,
            run_id=latest_run.run_id if latest_run else None,
            source_id=db_task.source_id,
            target_url=db_task.target_url,
            schema_id=db_task.schema_id,
            mode=db_task.mode,
            status=db_task.status,
            priority=db_task.priority,
            attempt=db_task.current_attempt,
            max_attempts=db_task.max_attempts,
            created_at=db_task.created_at,
            started_at=db_task.started_at,
            completed_at=db_task.completed_at,
            records_extracted=db_task.records_extracted,
            errors=[e.get("message", str(e)) for e in db_task.errors],
        )

    async def list(
        self,
        status: TaskStatus | None = None,
        source_id: str | None = None,
        schema_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[TaskDetail], int]:
        """List tasks with filters."""
        stmt = select(TaskModel)

        if status:
            stmt = stmt.where(TaskModel.status == status)
        if source_id:
            stmt = stmt.where(TaskModel.source_id == source_id)
        if schema_id:
            stmt = stmt.where(TaskModel.schema_id == schema_id)

        # Count total
        count_stmt = select(func.count(TaskModel.id)).where(stmt.whereclause or True)
        count_result = await self.db.execute(count_stmt)
        total = count_result.scalar() or 0

        # Apply pagination
        stmt = stmt.order_by(TaskModel.created_at.desc())
        stmt = stmt.limit(limit).offset(offset)

        result = await self.db.execute(stmt)
        tasks = result.scalars().all()

        task_details = []
        for db_task in tasks:
            task_details.append(
                TaskDetail(
                    task_id=db_task.id,
                    run_id=None,
                    source_id=db_task.source_id,
                    target_url=db_task.target_url,
                    schema_id=db_task.schema_id,
                    mode=db_task.mode,
                    status=db_task.status,
                    priority=db_task.priority,
                    attempt=db_task.current_attempt,
                    max_attempts=db_task.max_attempts,
                    created_at=db_task.created_at,
                    started_at=db_task.started_at,
                    completed_at=db_task.completed_at,
                    records_extracted=db_task.records_extracted,
                    errors=[e.get("message", str(e)) for e in db_task.errors],
                )
            )

        return task_details, total

    async def retry(self, task_id: UUID) -> bool:
        """Retry a failed task."""
        stmt = select(TaskModel).where(TaskModel.id == task_id)
        result = await self.db.execute(stmt)
        db_task = result.scalar_one_or_none()

        if not db_task:
            return False

        if db_task.status not in (TaskStatus.FAILED, TaskStatus.DLQ):
            logger.warning(
                "Cannot retry task with status",
                task_id=str(task_id),
                status=db_task.status,
            )
            return False

        # Reset and republish
        db_task.current_attempt = 0
        db_task.errors = []
        db_task.status = TaskStatus.PENDING

        task_message = TaskMessage(
            task_id=db_task.id,
            source_id=db_task.source_id,
            target_url=db_task.target_url,
            mode=db_task.mode,
            schema_id=db_task.schema_id,
            schema_version=db_task.schema_version,
            priority=db_task.priority,
            max_attempts=db_task.max_attempts,
            proxy_profile_id=db_task.proxy_profile_id,
            session_profile_id=db_task.session_profile_id,
            context=db_task.context,
        )

        await self._publish_task(task_message)
        db_task.status = TaskStatus.QUEUED

        await self.db.commit()

        logger.info("Retried task", task_id=str(task_id))
        return True

    async def cancel(self, task_id: UUID) -> bool:
        """Cancel a pending/queued task."""
        stmt = select(TaskModel).where(TaskModel.id == task_id)
        result = await self.db.execute(stmt)
        db_task = result.scalar_one_or_none()

        if not db_task:
            return False

        if db_task.status not in (TaskStatus.PENDING, TaskStatus.QUEUED):
            return False

        db_task.status = TaskStatus.CANCELLED
        db_task.completed_at = datetime.utcnow()

        await self.db.commit()

        logger.info("Cancelled task", task_id=str(task_id))
        return True

    async def update_from_result(
        self,
        task_id: UUID,
        run_id: UUID,
        status: str,
        metrics: dict[str, Any],
        extraction: dict[str, Any],
        errors: list[dict[str, Any]],
        pointers: dict[str, Any],
    ) -> None:
        """Update task from execution result."""
        stmt = select(TaskModel).where(TaskModel.id == task_id)
        result = await self.db.execute(stmt)
        db_task = result.scalar_one_or_none()

        if not db_task:
            logger.warning("Task not found for result update", task_id=str(task_id))
            return

        # Update task
        db_task.status = TaskStatus(status) if status in TaskStatus.__members__ else TaskStatus.FAILED
        db_task.records_extracted = extraction.get("records_extracted", 0)
        db_task.records_valid = extraction.get("records_valid", 0)
        db_task.delta_path = pointers.get("delta_path")
        db_task.errors = errors
        db_task.completed_at = datetime.utcnow()
        db_task.current_attempt += 1

        # Create run record
        run = TaskRunModel(
            task_id=task_id,
            run_id=run_id,
            attempt=db_task.current_attempt,
            status=status,
            http_status=metrics.get("http_status"),
            duration_ms=metrics.get("duration_ms"),
            bytes_downloaded=metrics.get("bytes_downloaded", 0),
            requests_count=metrics.get("requests_count", 0),
            pages_processed=metrics.get("pages_processed", 0),
            records_extracted=extraction.get("records_extracted", 0),
            records_valid=extraction.get("records_valid", 0),
            records_rejected=extraction.get("records_rejected", 0),
            delta_path=pointers.get("delta_path"),
            raw_html_path=pointers.get("raw_html_path"),
            screenshot_path=pointers.get("screenshot_path"),
            errors=errors,
            completed_at=datetime.utcnow(),
        )

        self.db.add(run)
        await self.db.commit()

        logger.info(
            "Updated task from result",
            task_id=str(task_id),
            run_id=str(run_id),
            status=status,
        )

    async def get_dlq_tasks(self, limit: int = 50) -> list[TaskDetail]:
        """Get tasks in Dead Letter Queue."""
        tasks, _ = await self.list(status=TaskStatus.DLQ, limit=limit)
        return tasks

    async def get_stats(self) -> dict[str, Any]:
        """Get task statistics."""
        # Count by status
        status_counts = {}
        for status in TaskStatus:
            count_stmt = select(func.count(TaskModel.id)).where(
                TaskModel.status == status
            )
            result = await self.db.execute(count_stmt)
            status_counts[status.value] = result.scalar() or 0

        # Today's tasks
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_stmt = select(func.count(TaskModel.id)).where(
            TaskModel.created_at >= today
        )
        today_result = await self.db.execute(today_stmt)
        today_count = today_result.scalar() or 0

        # Success rate today
        success_stmt = select(func.count(TaskModel.id)).where(
            TaskModel.created_at >= today,
            TaskModel.status == TaskStatus.SUCCESS,
        )
        success_result = await self.db.execute(success_stmt)
        success_count = success_result.scalar() or 0

        success_rate = (success_count / today_count * 100) if today_count > 0 else 0

        return {
            "by_status": status_counts,
            "today_total": today_count,
            "today_success": success_count,
            "success_rate": round(success_rate, 2),
        }

    async def _publish_task(self, task_message: TaskMessage) -> None:
        """Publish task to RabbitMQ."""
        client = await get_rmq_client()
        await client.publish_task(
            task=task_message.model_dump(mode="json"),
            mode=task_message.mode,
        )
