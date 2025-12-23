"""Delta Lake client for data lake operations."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa
import structlog
from deltalake import DeltaTable, write_deltalake

from src.config import DeltaLakeSettings, MinIOSettings, get_settings

logger = structlog.get_logger()


class DeltaWriter:
    """Writer for Delta Lake bronze and silver layers."""

    def __init__(
        self,
        delta_settings: DeltaLakeSettings | None = None,
        minio_settings: MinIOSettings | None = None,
    ):
        settings = get_settings()
        self.delta_settings = delta_settings or settings.delta
        self.minio_settings = minio_settings or settings.minio
        self._storage_options = self._get_storage_options()

    def _get_storage_options(self) -> dict[str, str]:
        """Get storage options for S3/MinIO."""
        return {
            "AWS_ENDPOINT_URL": f"http://{self.minio_settings.endpoint}",
            "AWS_ACCESS_KEY_ID": self.minio_settings.access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_settings.secret_key.get_secret_value(),
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def _get_partition_path(
        self,
        source_id: str,
        task_id: UUID | str,
        layer: str = "bronze",
    ) -> str:
        """Generate partition path for data."""
        now = datetime.utcnow()
        base_path = (
            self.delta_settings.bronze_path
            if layer == "bronze"
            else self.delta_settings.silver_path
        )

        # Path format: s3://bucket/delta/layer/source_id/year/month/day/task_id/
        return (
            f"{base_path}{source_id}/"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{task_id}/"
        )

    async def write_raw_records(
        self,
        records: list[dict[str, Any]],
        task_id: UUID | str,
        run_id: UUID | str,
        source_id: str,
        schema_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Write raw extracted records to bronze layer."""
        if not records:
            logger.warning("No records to write", task_id=str(task_id))
            return ""

        # Add metadata columns
        enriched_records = []
        now = datetime.utcnow()

        for i, record in enumerate(records):
            enriched_records.append({
                **record,
                "_task_id": str(task_id),
                "_run_id": str(run_id),
                "_source_id": source_id,
                "_schema_id": schema_id,
                "_record_index": i,
                "_ingested_at": now.isoformat(),
                "_metadata": json.dumps(metadata or {}),
            })

        # Convert to Polars DataFrame then to PyArrow
        df = pl.DataFrame(enriched_records)
        table = df.to_arrow()

        # Write to Delta Lake
        path = self._get_partition_path(source_id, task_id, "bronze")

        try:
            write_deltalake(
                path,
                table,
                mode="append",
                storage_options=self._storage_options,
                partition_by=["_source_id"],
            )

            logger.info(
                "Wrote records to Delta Lake",
                path=path,
                record_count=len(records),
                task_id=str(task_id),
            )

            return path

        except Exception as e:
            logger.error(
                "Failed to write to Delta Lake",
                path=path,
                error=str(e),
                task_id=str(task_id),
            )
            raise

    async def write_cleaned_records(
        self,
        records: list[dict[str, Any]],
        task_id: UUID | str,
        source_id: str,
        schema_id: str,
    ) -> str:
        """Write cleaned records to silver layer."""
        if not records:
            return ""

        # Add metadata
        now = datetime.utcnow()
        enriched_records = []

        for record in records:
            enriched_records.append({
                **record,
                "_task_id": str(task_id),
                "_source_id": source_id,
                "_schema_id": schema_id,
                "_cleaned_at": now.isoformat(),
            })

        df = pl.DataFrame(enriched_records)
        table = df.to_arrow()

        path = self._get_partition_path(source_id, task_id, "silver")

        write_deltalake(
            path,
            table,
            mode="append",
            storage_options=self._storage_options,
        )

        logger.info(
            "Wrote cleaned records to silver layer",
            path=path,
            record_count=len(records),
        )

        return path


class DeltaReader:
    """Reader for Delta Lake data."""

    def __init__(
        self,
        delta_settings: DeltaLakeSettings | None = None,
        minio_settings: MinIOSettings | None = None,
    ):
        settings = get_settings()
        self.delta_settings = delta_settings or settings.delta
        self.minio_settings = minio_settings or settings.minio
        self._storage_options = self._get_storage_options()

    def _get_storage_options(self) -> dict[str, str]:
        """Get storage options for S3/MinIO."""
        return {
            "AWS_ENDPOINT_URL": f"http://{self.minio_settings.endpoint}",
            "AWS_ACCESS_KEY_ID": self.minio_settings.access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_settings.secret_key.get_secret_value(),
            "AWS_REGION": "us-east-1",
        }

    def read_by_task(
        self,
        path: str,
        task_id: str | None = None,
    ) -> pl.DataFrame:
        """Read records from Delta Lake."""
        try:
            dt = DeltaTable(path, storage_options=self._storage_options)
            df = pl.from_arrow(dt.to_pyarrow_table())

            if task_id:
                df = df.filter(pl.col("_task_id") == task_id)

            return df

        except Exception as e:
            logger.error("Failed to read from Delta Lake", path=path, error=str(e))
            raise

    def read_by_source(
        self,
        source_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        layer: str = "bronze",
    ) -> pl.DataFrame:
        """Read records by source with optional date filtering."""
        base_path = (
            self.delta_settings.bronze_path
            if layer == "bronze"
            else self.delta_settings.silver_path
        )
        path = f"{base_path}{source_id}/"

        try:
            dt = DeltaTable(path, storage_options=self._storage_options)
            df = pl.from_arrow(dt.to_pyarrow_table())

            if start_date:
                df = df.filter(pl.col("_ingested_at") >= start_date.isoformat())
            if end_date:
                df = df.filter(pl.col("_ingested_at") <= end_date.isoformat())

            return df

        except Exception as e:
            logger.error(
                "Failed to read from Delta Lake",
                path=path,
                source_id=source_id,
                error=str(e),
            )
            return pl.DataFrame()


class TrashSwampWriter:
    """Writer for rejected/debug data to trash_swamp (S3/MinIO)."""

    def __init__(self, minio_settings: MinIOSettings | None = None):
        from minio import Minio

        settings = get_settings()
        self.settings = minio_settings or settings.minio

        self.client = Minio(
            self.settings.endpoint,
            access_key=self.settings.access_key,
            secret_key=self.settings.secret_key.get_secret_value(),
            secure=self.settings.secure,
        )

        # Ensure bucket exists
        if not self.client.bucket_exists(self.settings.bucket_trash):
            self.client.make_bucket(self.settings.bucket_trash)

    def write_rejected(
        self,
        data: list[dict[str, Any]],
        task_id: str,
        reason: str,
    ) -> str:
        """Write rejected records to trash_swamp."""
        import io

        now = datetime.utcnow()
        path = f"rejected/{now.year}/{now.month:02d}/{now.day:02d}/{task_id}.json"

        content = json.dumps({
            "task_id": task_id,
            "reason": reason,
            "rejected_at": now.isoformat(),
            "records": data,
        }, indent=2)

        self.client.put_object(
            self.settings.bucket_trash,
            path,
            io.BytesIO(content.encode()),
            length=len(content),
            content_type="application/json",
        )

        logger.info("Wrote rejected data to trash_swamp", path=path, count=len(data))
        return f"s3://{self.settings.bucket_trash}/{path}"

    def write_debug(
        self,
        task_id: str,
        html: str | None = None,
        screenshot: bytes | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Write debug artifacts to trash_swamp."""
        import io

        now = datetime.utcnow()
        base_path = f"debug/{now.year}/{now.month:02d}/{now.day:02d}/{task_id}"
        paths = {}

        if html:
            html_path = f"{base_path}/page.html"
            self.client.put_object(
                self.settings.bucket_trash,
                html_path,
                io.BytesIO(html.encode()),
                length=len(html),
                content_type="text/html",
            )
            paths["html"] = f"s3://{self.settings.bucket_trash}/{html_path}"

        if screenshot:
            screenshot_path = f"{base_path}/screenshot.png"
            self.client.put_object(
                self.settings.bucket_trash,
                screenshot_path,
                io.BytesIO(screenshot),
                length=len(screenshot),
                content_type="image/png",
            )
            paths["screenshot"] = f"s3://{self.settings.bucket_trash}/{screenshot_path}"

        if metadata:
            meta_path = f"{base_path}/metadata.json"
            content = json.dumps(metadata, indent=2, default=str)
            self.client.put_object(
                self.settings.bucket_trash,
                meta_path,
                io.BytesIO(content.encode()),
                length=len(content),
                content_type="application/json",
            )
            paths["metadata"] = f"s3://{self.settings.bucket_trash}/{meta_path}"

        return paths
