"""PostgreSQL Loader - Load cleaned data into PostgreSQL."""

import asyncio
from datetime import datetime
from typing import Any

import polars as pl
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, Text, Boolean
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
import structlog

from src.config import get_settings
from src.shared.delta_client import DeltaReader

logger = structlog.get_logger()


class PostgreSQLLoader:
    """Service for loading cleaned data into PostgreSQL."""

    def __init__(self):
        settings = get_settings()
        self._engine: Engine = create_engine(settings.db.sync_url)
        self._delta_reader = DeltaReader()
        self._metadata = MetaData()

    def load_source(
        self,
        source_id: str,
        table_name: str | None = None,
        upsert: bool = True,
        upsert_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """Load data from silver layer into PostgreSQL.

        Args:
            source_id: Source identifier
            table_name: Target table name (auto-generated if not provided)
            upsert: Whether to upsert (update on conflict) or insert only
            upsert_keys: Columns to use for conflict detection

        Returns:
            Loading statistics
        """
        logger.info("Starting PostgreSQL load", source_id=source_id, table=table_name)

        # Read from silver layer
        df = self._delta_reader.read_by_source(source_id, layer="silver")

        if df.is_empty():
            logger.warning("No data to load", source_id=source_id)
            return {"status": "no_data", "records_loaded": 0}

        # Generate table name if not provided
        if not table_name:
            table_name = self._generate_table_name(source_id)

        # Remove metadata columns for target table
        data_columns = [c for c in df.columns if not c.startswith("_")]
        df = df.select(data_columns)

        # Ensure table exists
        self._ensure_table(table_name, df)

        # Load data
        records_loaded = self._load_data(
            df=df,
            table_name=table_name,
            upsert=upsert,
            upsert_keys=upsert_keys or self._detect_upsert_keys(df),
        )

        logger.info(
            "PostgreSQL load complete",
            source_id=source_id,
            table=table_name,
            records=records_loaded,
        )

        return {
            "status": "success",
            "table_name": table_name,
            "records_loaded": records_loaded,
        }

    def _generate_table_name(self, source_id: str) -> str:
        """Generate a valid table name from source ID."""
        # Convert source_id to valid table name
        name = source_id.lower()
        name = name.replace(".", "_").replace("/", "_").replace("-", "_")
        name = "data_" + name

        # Truncate if too long
        if len(name) > 63:
            name = name[:63]

        return name

    def _ensure_table(self, table_name: str, df: pl.DataFrame) -> None:
        """Create table if it doesn't exist."""
        inspector = inspect(self._engine)

        if inspector.has_table(table_name):
            logger.debug("Table exists", table=table_name)
            return

        # Build table schema from DataFrame
        columns = [
            Column("id", Integer, primary_key=True, autoincrement=True),
        ]

        for col_name in df.columns:
            dtype = df[col_name].dtype
            sql_type = self._polars_to_sqlalchemy_type(dtype)
            columns.append(Column(col_name, sql_type))

        # Add metadata columns
        columns.extend([
            Column("_loaded_at", DateTime, default=datetime.utcnow),
        ])

        table = Table(table_name, self._metadata, *columns)
        table.create(self._engine)

        logger.info("Created table", table=table_name, columns=len(columns))

    def _polars_to_sqlalchemy_type(self, dtype: pl.DataType):
        """Convert Polars dtype to SQLAlchemy type."""
        if dtype == pl.Utf8:
            return Text
        if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            return Integer
        if dtype in (pl.Float32, pl.Float64):
            return Float
        if dtype == pl.Boolean:
            return Boolean
        if dtype == pl.Datetime:
            return DateTime
        if dtype == pl.Date:
            return DateTime

        return Text  # Default to Text

    def _detect_upsert_keys(self, df: pl.DataFrame) -> list[str]:
        """Detect likely unique key columns for upsert."""
        candidates = []

        for col in df.columns:
            col_lower = col.lower()
            if any(kw in col_lower for kw in ["id", "sku", "url", "code"]):
                candidates.append(col)

        return candidates[:2] if candidates else []

    def _load_data(
        self,
        df: pl.DataFrame,
        table_name: str,
        upsert: bool,
        upsert_keys: list[str],
    ) -> int:
        """Load data into the table."""
        records = df.to_dicts()

        if not records:
            return 0

        # Add loading timestamp
        now = datetime.utcnow()
        for record in records:
            record["_loaded_at"] = now

        # Get table reference
        table = Table(table_name, self._metadata, autoload_with=self._engine)

        with self._engine.begin() as conn:
            if upsert and upsert_keys:
                # Use upsert (INSERT ... ON CONFLICT)
                stmt = insert(table).values(records)

                update_columns = {
                    c.name: c for c in stmt.excluded
                    if c.name not in upsert_keys and c.name != "id"
                }

                stmt = stmt.on_conflict_do_update(
                    index_elements=upsert_keys,
                    set_=update_columns,
                )

                conn.execute(stmt)
            else:
                # Simple insert
                conn.execute(table.insert().values(records))

        return len(records)

    def create_index(
        self,
        table_name: str,
        columns: list[str],
        unique: bool = False,
    ) -> None:
        """Create an index on a table."""
        from sqlalchemy import Index

        index_name = f"idx_{table_name}_{'_'.join(columns)}"

        table = Table(table_name, self._metadata, autoload_with=self._engine)
        index = Index(
            index_name,
            *[table.c[col] for col in columns],
            unique=unique,
        )

        index.create(self._engine)
        logger.info("Created index", index=index_name, columns=columns)

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Get statistics for a loaded table."""
        with self._engine.connect() as conn:
            result = conn.execute(
                f"SELECT COUNT(*) as count, MAX(_loaded_at) as last_load "
                f"FROM {table_name}"
            )
            row = result.fetchone()

            return {
                "table_name": table_name,
                "record_count": row[0],
                "last_loaded_at": row[1].isoformat() if row[1] else None,
            }


async def main():
    """Main entry point for PostgreSQL loader."""
    import argparse

    parser = argparse.ArgumentParser(description="PostgreSQL Loader")
    parser.add_argument("--source-id", required=True, help="Source ID to load")
    parser.add_argument("--table", help="Target table name")
    parser.add_argument("--no-upsert", action="store_true", help="Disable upsert")

    args = parser.parse_args()

    loader = PostgreSQLLoader()
    result = loader.load_source(
        source_id=args.source_id,
        table_name=args.table,
        upsert=not args.no_upsert,
    )

    print(f"Loading result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
