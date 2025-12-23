"""Cure Data Parser - Data cleaning and normalization service."""

import asyncio
import re
from datetime import datetime
from typing import Any

import polars as pl
import structlog

from src.config import get_settings
from src.shared.delta_client import DeltaReader, DeltaWriter, TrashSwampWriter

logger = structlog.get_logger()


class CureDataParser:
    """Service for cleaning and normalizing extracted data."""

    def __init__(self):
        settings = get_settings()
        self._delta_reader = DeltaReader()
        self._delta_writer = DeltaWriter()
        self._trash_writer = TrashSwampWriter()

    async def process_source(
        self,
        source_id: str,
        schema_id: str,
        cleaning_rules: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Process all data for a source.

        Args:
            source_id: Source identifier
            schema_id: Schema identifier
            cleaning_rules: Optional cleaning rules override

        Returns:
            Processing statistics
        """
        logger.info(
            "Starting cure processing",
            source_id=source_id,
            schema_id=schema_id,
        )

        # Read from bronze layer
        df = self._delta_reader.read_by_source(source_id, layer="bronze")

        if df.is_empty():
            logger.warning("No data found", source_id=source_id)
            return {"status": "no_data", "records_processed": 0}

        original_count = len(df)

        # Apply cleaning pipeline
        df = self._remove_duplicates(df, schema_id)
        df = self._clean_fields(df, cleaning_rules)
        df = self._validate_records(df)
        df = self._normalize_types(df)

        valid_count = len(df)
        rejected_count = original_count - valid_count

        # Write to silver layer
        if not df.is_empty():
            records = df.to_dicts()
            await self._delta_writer.write_cleaned_records(
                records=records,
                task_id=f"cure_{source_id}_{datetime.utcnow().isoformat()}",
                source_id=source_id,
                schema_id=schema_id,
            )

        logger.info(
            "Cure processing complete",
            source_id=source_id,
            original=original_count,
            valid=valid_count,
            rejected=rejected_count,
        )

        return {
            "status": "success",
            "records_original": original_count,
            "records_valid": valid_count,
            "records_rejected": rejected_count,
        }

    def _remove_duplicates(
        self,
        df: pl.DataFrame,
        schema_id: str,
    ) -> pl.DataFrame:
        """Remove duplicate records based on schema dedup keys."""
        # Get dedup keys from schema (simplified - in production, fetch from API)
        # For now, use common fields
        dedup_columns = []

        for col in df.columns:
            if col in ["title", "name", "url", "id", "sku", "product_id"]:
                dedup_columns.append(col)

        if not dedup_columns:
            # Default to all non-metadata columns
            dedup_columns = [c for c in df.columns if not c.startswith("_")]

        if dedup_columns:
            before_count = len(df)
            df = df.unique(subset=dedup_columns, keep="first")
            logger.debug(
                "Removed duplicates",
                before=before_count,
                after=len(df),
                columns=dedup_columns,
            )

        return df

    def _clean_fields(
        self,
        df: pl.DataFrame,
        rules: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """Apply field-level cleaning."""
        rules = rules or {}

        for col in df.columns:
            if col.startswith("_"):
                continue  # Skip metadata columns

            dtype = df[col].dtype

            # String cleaning
            if dtype == pl.Utf8:
                df = df.with_columns([
                    pl.col(col)
                    .str.strip_chars()
                    .str.replace_all(r"\s+", " ")
                    .alias(col)
                ])

            # Apply custom rules
            if col in rules:
                rule = rules[col]

                if rule.get("remove_html"):
                    df = df.with_columns([
                        pl.col(col)
                        .str.replace_all(r"<[^>]+>", "")
                        .alias(col)
                    ])

                if rule.get("lowercase"):
                    df = df.with_columns([
                        pl.col(col).str.to_lowercase().alias(col)
                    ])

                if rule.get("extract_pattern"):
                    pattern = rule["extract_pattern"]
                    df = df.with_columns([
                        pl.col(col)
                        .str.extract(pattern, 0)
                        .alias(col)
                    ])

        return df

    def _validate_records(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate records and filter invalid ones."""
        # Remove records with all null values (except metadata)
        data_columns = [c for c in df.columns if not c.startswith("_")]

        if data_columns:
            # Keep rows with at least one non-null data column
            mask = pl.lit(False)
            for col in data_columns:
                mask = mask | pl.col(col).is_not_null()

            df = df.filter(mask)

        # Remove records with empty string values in required fields
        for col in ["title", "name"]:
            if col in df.columns:
                df = df.filter(
                    (pl.col(col).is_not_null()) &
                    (pl.col(col).str.len_chars() > 0)
                )

        return df

    def _normalize_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize data types for PostgreSQL compatibility."""
        for col in df.columns:
            if col.startswith("_"):
                continue

            # Normalize price/numeric columns
            if any(kw in col.lower() for kw in ["price", "cost", "amount", "total"]):
                try:
                    df = df.with_columns([
                        pl.col(col)
                        .cast(pl.Float64, strict=False)
                        .alias(col)
                    ])
                except Exception:
                    pass

            # Normalize boolean columns
            if any(kw in col.lower() for kw in ["is_", "has_", "available", "in_stock"]):
                try:
                    df = df.with_columns([
                        pl.col(col)
                        .cast(pl.Boolean, strict=False)
                        .alias(col)
                    ])
                except Exception:
                    pass

            # Normalize date columns
            if any(kw in col.lower() for kw in ["date", "time", "created", "updated"]):
                try:
                    df = df.with_columns([
                        pl.col(col)
                        .str.to_datetime(strict=False)
                        .alias(col)
                    ])
                except Exception:
                    pass

        return df


async def main():
    """Main entry point for cure data parser."""
    import argparse

    parser = argparse.ArgumentParser(description="Cure Data Parser")
    parser.add_argument("--source-id", required=True, help="Source ID to process")
    parser.add_argument("--schema-id", required=True, help="Schema ID")

    args = parser.parse_args()

    cure_parser = CureDataParser()
    result = await cure_parser.process_source(
        source_id=args.source_id,
        schema_id=args.schema_id,
    )

    print(f"Processing result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
