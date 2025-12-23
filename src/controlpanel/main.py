"""Main FastAPI application for ControlPanel."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog

from src.config import get_settings
from src.controlpanel.api import ai_router, schemas_router, stats_router, tasks_router
from src.controlpanel.database import close_db, init_db
from src.shared.rmq_client import close_rmq_client

settings = get_settings()
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    # Startup
    logger.info("Starting ControlPanel API", version=settings.app_version)

    try:
        await init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        raise

    yield

    # Shutdown
    logger.info("Shutting down ControlPanel API")
    await close_db()
    await close_rmq_client()


app = FastAPI(
    title=settings.app_name,
    description="""
    Universal Parser Control Panel API

    Manage parsing schemas, tasks, and monitor data collection pipelines.

    ## Features

    * **Schemas** - Create and manage parsing schemas that define what data to extract
    * **Tasks** - Create, monitor, and manage parsing tasks
    * **AI** - AI-powered schema generation and validation
    * **Statistics** - Monitor system health and task statistics

    ## Authentication

    Currently running without authentication (development mode).
    """,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(
        "Unhandled exception",
        path=request.url.path,
        method=request.method,
        error=str(exc),
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# Include routers
app.include_router(schemas_router, prefix=settings.api_prefix)
app.include_router(tasks_router, prefix=settings.api_prefix)
app.include_router(stats_router, prefix=settings.api_prefix)
app.include_router(ai_router, prefix=settings.api_prefix)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "health": f"{settings.api_prefix}/stats/health",
    }


@app.get("/health")
async def health():
    """Quick health check endpoint."""
    return {"status": "healthy"}


def create_app() -> FastAPI:
    """Factory function for creating the app."""
    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.controlpanel.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
