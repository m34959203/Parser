"""Service for managing parsing schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from src.controlpanel.models import ParsingSchemaModel, SchemaVersionModel
from src.shared.models import ParsingSchema, ParsingSchemaCreate, ParsingSchemaUpdate

logger = structlog.get_logger()


class SchemaService:
    """Service for CRUD operations on parsing schemas."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(
        self,
        schema_data: ParsingSchemaCreate,
        created_by: str | None = None,
    ) -> ParsingSchema:
        """Create a new parsing schema."""
        # Generate schema_id
        schema_id = f"{schema_data.source_id.replace('/', '_').replace('.', '_')}_{uuid4().hex[:8]}"

        # Create database model
        db_schema = ParsingSchemaModel(
            schema_id=schema_id,
            source_id=schema_data.source_id,
            description=schema_data.description,
            current_version="1.0.0",
            start_url=schema_data.start_url,
            url_pattern=schema_data.url_pattern,
            item_container=schema_data.item_container,
            fields=[f.model_dump() for f in schema_data.fields],
            navigation_steps=[s.model_dump() for s in schema_data.navigation_steps],
            pagination=schema_data.pagination.model_dump() if schema_data.pagination else None,
            min_fields_required=schema_data.min_fields_required,
            dedup_keys=schema_data.dedup_keys,
            mode=schema_data.mode,
            requires_js=schema_data.requires_js,
            request_headers=schema_data.request_headers,
            tags=schema_data.tags,
            created_by=created_by,
        )

        self.db.add(db_schema)
        await self.db.flush()

        # Create initial version
        version = SchemaVersionModel(
            schema_uuid=db_schema.id,
            version="1.0.0",
            schema_data=db_schema.to_dict(),
            change_description="Initial version",
            created_by=created_by,
        )
        self.db.add(version)

        await self.db.commit()
        await self.db.refresh(db_schema)

        logger.info(
            "Created parsing schema",
            schema_id=schema_id,
            source_id=schema_data.source_id,
        )

        return self._to_pydantic(db_schema)

    async def get(
        self,
        schema_id: str,
        version: str | None = None,
    ) -> ParsingSchema | None:
        """Get a parsing schema by ID."""
        stmt = select(ParsingSchemaModel).where(
            ParsingSchemaModel.schema_id == schema_id
        )
        result = await self.db.execute(stmt)
        db_schema = result.scalar_one_or_none()

        if not db_schema:
            return None

        if version and version != db_schema.current_version:
            # Get specific version from history
            version_stmt = select(SchemaVersionModel).where(
                SchemaVersionModel.schema_uuid == db_schema.id,
                SchemaVersionModel.version == version,
            )
            version_result = await self.db.execute(version_stmt)
            schema_version = version_result.scalar_one_or_none()

            if schema_version:
                return ParsingSchema(**schema_version.schema_data)
            return None

        return self._to_pydantic(db_schema)

    async def list(
        self,
        source_id: str | None = None,
        is_active: bool | None = None,
        tags: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[ParsingSchema], int]:
        """List parsing schemas with filters."""
        stmt = select(ParsingSchemaModel)

        if source_id:
            stmt = stmt.where(ParsingSchemaModel.source_id == source_id)
        if is_active is not None:
            stmt = stmt.where(ParsingSchemaModel.is_active == is_active)
        if tags:
            # Filter by any matching tag
            stmt = stmt.where(ParsingSchemaModel.tags.op("&&")(tags))

        # Count total
        count_stmt = select(ParsingSchemaModel.id).where(stmt.whereclause or True)
        count_result = await self.db.execute(count_stmt)
        total = len(count_result.all())

        # Apply pagination
        stmt = stmt.order_by(ParsingSchemaModel.updated_at.desc())
        stmt = stmt.limit(limit).offset(offset)

        result = await self.db.execute(stmt)
        schemas = result.scalars().all()

        return [self._to_pydantic(s) for s in schemas], total

    async def update(
        self,
        schema_id: str,
        update_data: ParsingSchemaUpdate,
        updated_by: str | None = None,
    ) -> ParsingSchema | None:
        """Update a parsing schema (creates new version)."""
        stmt = select(ParsingSchemaModel).where(
            ParsingSchemaModel.schema_id == schema_id
        )
        result = await self.db.execute(stmt)
        db_schema = result.scalar_one_or_none()

        if not db_schema:
            return None

        # Update fields
        update_dict = update_data.model_dump(exclude_unset=True)

        if "fields" in update_dict:
            update_dict["fields"] = [f.model_dump() for f in update_data.fields]
        if "navigation_steps" in update_dict and update_data.navigation_steps:
            update_dict["navigation_steps"] = [s.model_dump() for s in update_data.navigation_steps]
        if "pagination" in update_dict and update_data.pagination:
            update_dict["pagination"] = update_data.pagination.model_dump()

        for key, value in update_dict.items():
            setattr(db_schema, key, value)

        # Increment version
        old_version = db_schema.current_version
        parts = old_version.split(".")
        parts[-1] = str(int(parts[-1]) + 1)
        new_version = ".".join(parts)
        db_schema.current_version = new_version
        db_schema.updated_at = datetime.utcnow()

        # Save version history
        version = SchemaVersionModel(
            schema_uuid=db_schema.id,
            version=new_version,
            schema_data=db_schema.to_dict(),
            change_description=f"Updated from {old_version}",
            created_by=updated_by,
        )
        self.db.add(version)

        await self.db.commit()
        await self.db.refresh(db_schema)

        logger.info(
            "Updated parsing schema",
            schema_id=schema_id,
            old_version=old_version,
            new_version=new_version,
        )

        return self._to_pydantic(db_schema)

    async def delete(
        self,
        schema_id: str,
        version: str | None = None,
    ) -> bool:
        """Delete a schema or specific version."""
        stmt = select(ParsingSchemaModel).where(
            ParsingSchemaModel.schema_id == schema_id
        )
        result = await self.db.execute(stmt)
        db_schema = result.scalar_one_or_none()

        if not db_schema:
            return False

        if version:
            # Delete specific version
            version_stmt = select(SchemaVersionModel).where(
                SchemaVersionModel.schema_uuid == db_schema.id,
                SchemaVersionModel.version == version,
            )
            version_result = await self.db.execute(version_stmt)
            schema_version = version_result.scalar_one_or_none()

            if schema_version:
                await self.db.delete(schema_version)
                await self.db.commit()
                return True
            return False

        # Delete entire schema
        await self.db.delete(db_schema)
        await self.db.commit()

        logger.info("Deleted parsing schema", schema_id=schema_id)
        return True

    async def list_versions(self, schema_id: str) -> list[dict[str, Any]]:
        """List all versions of a schema."""
        stmt = select(ParsingSchemaModel).where(
            ParsingSchemaModel.schema_id == schema_id
        )
        result = await self.db.execute(stmt)
        db_schema = result.scalar_one_or_none()

        if not db_schema:
            return []

        versions_stmt = select(SchemaVersionModel).where(
            SchemaVersionModel.schema_uuid == db_schema.id
        ).order_by(SchemaVersionModel.created_at.desc())

        versions_result = await self.db.execute(versions_stmt)
        versions = versions_result.scalars().all()

        return [
            {
                "version": v.version,
                "change_description": v.change_description,
                "created_by": v.created_by,
                "created_at": v.created_at.isoformat(),
            }
            for v in versions
        ]

    def _to_pydantic(self, db_schema: ParsingSchemaModel) -> ParsingSchema:
        """Convert database model to Pydantic model."""
        return ParsingSchema(
            schema_id=db_schema.schema_id,
            version=db_schema.current_version,
            source_id=db_schema.source_id,
            description=db_schema.description,
            start_url=db_schema.start_url,
            url_pattern=db_schema.url_pattern,
            item_container=db_schema.item_container,
            fields=db_schema.fields,
            navigation_steps=db_schema.navigation_steps,
            pagination=db_schema.pagination,
            min_fields_required=db_schema.min_fields_required,
            dedup_keys=db_schema.dedup_keys,
            mode=db_schema.mode,
            requires_js=db_schema.requires_js,
            request_headers=db_schema.request_headers,
            is_active=db_schema.is_active,
            confidence=db_schema.confidence,
            tags=db_schema.tags,
            created_by=db_schema.created_by,
            created_at=db_schema.created_at,
            updated_at=db_schema.updated_at,
        )
