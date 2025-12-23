"""Initial schema

Revision ID: 0001
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create parsing_schemas table
    op.create_table(
        'parsing_schemas',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('schema_id', sa.String(100), nullable=False),
        sa.Column('source_id', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('current_version', sa.String(20), nullable=True),
        sa.Column('start_url', sa.Text(), nullable=False),
        sa.Column('url_pattern', sa.Text(), nullable=True),
        sa.Column('item_container', sa.String(500), nullable=True),
        sa.Column('fields', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('navigation_steps', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('pagination', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('min_fields_required', sa.Integer(), nullable=True),
        sa.Column('dedup_keys', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('mode', sa.String(20), nullable=True),
        sa.Column('requires_js', sa.Boolean(), nullable=True),
        sa.Column('request_headers', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('tags', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_by', sa.String(100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_parsing_schemas')),
        sa.UniqueConstraint('schema_id', name=op.f('uq_parsing_schemas_schema_id'))
    )
    op.create_index(op.f('ix_parsing_schemas_is_active'), 'parsing_schemas', ['is_active'], unique=False)
    op.create_index(op.f('ix_parsing_schemas_schema_id'), 'parsing_schemas', ['schema_id'], unique=False)
    op.create_index(op.f('ix_parsing_schemas_source_id'), 'parsing_schemas', ['source_id'], unique=False)
    op.create_index('ix_parsing_schemas_source_active', 'parsing_schemas', ['source_id', 'is_active'], unique=False)

    # Create schema_versions table
    op.create_table(
        'schema_versions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('schema_uuid', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('version', sa.String(20), nullable=False),
        sa.Column('schema_data', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('change_description', sa.Text(), nullable=True),
        sa.Column('created_by', sa.String(100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['schema_uuid'], ['parsing_schemas.id'], name=op.f('fk_schema_versions_schema_uuid_parsing_schemas'), ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_schema_versions'))
    )
    op.create_index(op.f('ix_schema_versions_schema_uuid'), 'schema_versions', ['schema_uuid'], unique=False)
    op.create_index('ix_schema_versions_schema_version', 'schema_versions', ['schema_uuid', 'version'], unique=True)

    # Create tasks table
    op.create_table(
        'tasks',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_id', sa.String(255), nullable=False),
        sa.Column('target_url', sa.Text(), nullable=False),
        sa.Column('schema_id', sa.String(100), nullable=False),
        sa.Column('schema_version', sa.String(20), nullable=True),
        sa.Column('mode', sa.String(20), nullable=True),
        sa.Column('status', sa.Enum('PENDING', 'QUEUED', 'RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'RETRY', 'CANCELLED', 'DLQ', name='taskstatus'), nullable=True),
        sa.Column('priority', sa.Integer(), nullable=True),
        sa.Column('max_attempts', sa.Integer(), nullable=True),
        sa.Column('current_attempt', sa.Integer(), nullable=True),
        sa.Column('proxy_profile_id', sa.String(100), nullable=True),
        sa.Column('session_profile_id', sa.String(100), nullable=True),
        sa.Column('context', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('parent_task_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('records_extracted', sa.Integer(), nullable=True),
        sa.Column('records_valid', sa.Integer(), nullable=True),
        sa.Column('delta_path', sa.Text(), nullable=True),
        sa.Column('errors', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_tasks'))
    )
    op.create_index(op.f('ix_tasks_parent_task_id'), 'tasks', ['parent_task_id'], unique=False)
    op.create_index(op.f('ix_tasks_schema_id'), 'tasks', ['schema_id'], unique=False)
    op.create_index(op.f('ix_tasks_source_id'), 'tasks', ['source_id'], unique=False)
    op.create_index(op.f('ix_tasks_status'), 'tasks', ['status'], unique=False)
    op.create_index('ix_tasks_status_created', 'tasks', ['status', 'created_at'], unique=False)
    op.create_index('ix_tasks_source_status', 'tasks', ['source_id', 'status'], unique=False)

    # Create task_runs table
    op.create_table(
        'task_runs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('task_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('attempt', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(20), nullable=True),
        sa.Column('http_status', sa.Integer(), nullable=True),
        sa.Column('duration_ms', sa.Integer(), nullable=True),
        sa.Column('bytes_downloaded', sa.Integer(), nullable=True),
        sa.Column('requests_count', sa.Integer(), nullable=True),
        sa.Column('pages_processed', sa.Integer(), nullable=True),
        sa.Column('records_extracted', sa.Integer(), nullable=True),
        sa.Column('records_valid', sa.Integer(), nullable=True),
        sa.Column('records_rejected', sa.Integer(), nullable=True),
        sa.Column('delta_path', sa.Text(), nullable=True),
        sa.Column('raw_html_path', sa.Text(), nullable=True),
        sa.Column('screenshot_path', sa.Text(), nullable=True),
        sa.Column('errors', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('worker_id', sa.String(100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], name=op.f('fk_task_runs_task_id_tasks'), ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_task_runs')),
        sa.UniqueConstraint('run_id', name=op.f('uq_task_runs_run_id'))
    )
    op.create_index(op.f('ix_task_runs_task_id'), 'task_runs', ['task_id'], unique=False)
    op.create_index('ix_task_runs_task_attempt', 'task_runs', ['task_id', 'attempt'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_task_runs_task_attempt', table_name='task_runs')
    op.drop_index(op.f('ix_task_runs_task_id'), table_name='task_runs')
    op.drop_table('task_runs')

    op.drop_index('ix_tasks_source_status', table_name='tasks')
    op.drop_index('ix_tasks_status_created', table_name='tasks')
    op.drop_index(op.f('ix_tasks_status'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_source_id'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_schema_id'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_parent_task_id'), table_name='tasks')
    op.drop_table('tasks')

    op.drop_index('ix_schema_versions_schema_version', table_name='schema_versions')
    op.drop_index(op.f('ix_schema_versions_schema_uuid'), table_name='schema_versions')
    op.drop_table('schema_versions')

    op.drop_index('ix_parsing_schemas_source_active', table_name='parsing_schemas')
    op.drop_index(op.f('ix_parsing_schemas_source_id'), table_name='parsing_schemas')
    op.drop_index(op.f('ix_parsing_schemas_schema_id'), table_name='parsing_schemas')
    op.drop_index(op.f('ix_parsing_schemas_is_active'), table_name='parsing_schemas')
    op.drop_table('parsing_schemas')

    # Drop enum
    op.execute('DROP TYPE IF EXISTS taskstatus')
