#!/bin/bash
# Start script for Railway deployment

set -e

# Run database migrations (skip if no database URL)
if [ -n "$DATABASE_URL" ]; then
    echo "Running database migrations..."
    alembic upgrade head || echo "Migration skipped or failed"
fi

# Start the application
echo "Starting application on port ${PORT:-8000}..."
exec uvicorn src.controlpanel.main:app --host 0.0.0.0 --port ${PORT:-8000}
