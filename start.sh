#!/bin/bash
# Start script for Railway deployment

set -e

# Run database migrations
alembic upgrade head

# Start the application
exec uvicorn src.controlpanel.main:app --host 0.0.0.0 --port ${PORT:-8000}
