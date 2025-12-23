"""API routers for ControlPanel."""

from .schemas import router as schemas_router
from .tasks import router as tasks_router
from .stats import router as stats_router
from .ai import router as ai_router

__all__ = ["schemas_router", "tasks_router", "stats_router", "ai_router"]
