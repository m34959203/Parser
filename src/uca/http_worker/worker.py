"""HTTP Worker - Lightweight async web scraper."""

import asyncio
import json
import os
import signal
from datetime import datetime
from typing import Any
from uuid import UUID

import aiohttp
from aio_pika import IncomingMessage
import structlog

from src.config import WorkerSettings, get_settings
from src.shared.delta_client import DeltaWriter, TrashSwampWriter
from src.shared.models import ErrorDetail, ParsingSchema, TaskMessage
from src.shared.rmq_client import RabbitMQClient
from src.uca.common import DataExtractor, ResultBuilder

logger = structlog.get_logger()


class HTTPWorker:
    """Async HTTP worker for web scraping tasks."""

    def __init__(
        self,
        worker_id: str | None = None,
        settings: WorkerSettings | None = None,
    ):
        self.settings = settings or get_settings().worker
        self.worker_id = worker_id or f"http-worker-{os.getpid()}"

        self._rmq_client: RabbitMQClient | None = None
        self._session: aiohttp.ClientSession | None = None
        self._delta_writer: DeltaWriter | None = None
        self._trash_writer: TrashSwampWriter | None = None
        self._schemas_cache: dict[str, ParsingSchema] = {}
        self._running = False
        self._tasks_processed = 0

    async def start(self) -> None:
        """Start the worker."""
        logger.info("Starting HTTP Worker", worker_id=self.worker_id)

        # Initialize clients
        self._rmq_client = RabbitMQClient()
        await self._rmq_client.connect()

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.settings.request_timeout),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

        self._delta_writer = DeltaWriter()
        self._trash_writer = TrashSwampWriter()

        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(self.stop())
            )

        self._running = True

        # Start consuming
        await self._rmq_client.consume(
            queue_name="tasks.http",
            callback=self._process_message,
            prefetch_count=self.settings.http_prefetch,
        )

        logger.info("HTTP Worker started, waiting for tasks", worker_id=self.worker_id)

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        logger.info("Stopping HTTP Worker", worker_id=self.worker_id)
        self._running = False

        if self._session:
            await self._session.close()

        if self._rmq_client:
            await self._rmq_client.close()

        logger.info(
            "HTTP Worker stopped",
            worker_id=self.worker_id,
            tasks_processed=self._tasks_processed,
        )

    async def _process_message(self, message: IncomingMessage) -> None:
        """Process a single task message."""
        async with message.process():
            try:
                task = TaskMessage.model_validate_json(message.body)
                logger.info(
                    "Processing task",
                    task_id=str(task.task_id),
                    url=task.target_url,
                    attempt=task.attempt,
                )

                result = await self._execute_task(task)

                # Publish result
                await self._rmq_client.publish_result(
                    result.model_dump(mode="json")
                )

                self._tasks_processed += 1

            except Exception as e:
                logger.exception("Failed to process message", error=str(e))

    async def _execute_task(self, task: TaskMessage) -> Any:
        """Execute a scraping task."""
        result_builder = ResultBuilder(task.task_id, task.run_id)
        result_builder.set_started()
        result_builder.set_worker_id(self.worker_id)

        try:
            # Get schema
            schema = await self._get_schema(task.schema_id, task.schema_version)

            if not schema:
                result_builder.add_error(
                    code=ErrorDetail.Codes.VALIDATION_ERROR,
                    message=f"Schema '{task.schema_id}' not found",
                    is_retryable=False,
                )
                return result_builder.build_failed()

            # Fetch page
            html, http_status = await self._fetch_page(
                url=task.target_url,
                headers=task.headers or schema.request_headers,
                proxy=self._get_proxy(task.proxy_profile_id),
            )

            result_builder.set_http_status(http_status)
            result_builder.add_bytes_downloaded(len(html) if html else 0)
            result_builder.increment_requests()

            if not html or http_status >= 400:
                result_builder.add_error(
                    code=ErrorDetail.Codes.HTTP_ERROR,
                    message=f"HTTP {http_status}",
                    is_retryable=http_status in (429, 500, 502, 503, 504),
                )
                return result_builder.build_failed()

            # Extract data
            extractor = DataExtractor(schema, base_url=task.target_url)
            records = extractor.extract(html)

            valid_records = [r for r in records if r]
            rejected_count = len(records) - len(valid_records)

            result_builder.set_extraction_stats(
                extracted=len(records),
                valid=len(valid_records),
                rejected=rejected_count,
            )
            result_builder.increment_pages()

            # Save to Delta Lake
            if valid_records:
                delta_path = await self._delta_writer.write_raw_records(
                    records=valid_records,
                    task_id=task.task_id,
                    run_id=task.run_id,
                    source_id=task.source_id,
                    schema_id=task.schema_id,
                )
                result_builder.set_delta_path(delta_path)

            # Save rejected to trash
            if rejected_count > 0:
                rejected_records = [r for r in records if not r]
                self._trash_writer.write_rejected(
                    data=rejected_records,
                    task_id=str(task.task_id),
                    reason="Validation failed",
                )

            # Check pagination
            if schema.pagination:
                next_url = self._get_next_page_url(html, schema, task)
                if next_url and task.page_number < (task.max_pages or schema.pagination.max_pages):
                    result_builder.set_pagination(
                        has_next=True,
                        next_url=next_url,
                        current_page=task.page_number,
                    )

                    # Create child task for next page
                    await self._create_pagination_task(task, next_url)

            return result_builder.build_success()

        except asyncio.TimeoutError:
            result_builder.add_error(
                code=ErrorDetail.Codes.TIMEOUT,
                message="Request timeout",
                is_retryable=True,
            )
            return result_builder.build_retry() if task.attempt < task.max_attempts else result_builder.build_failed()

        except aiohttp.ClientError as e:
            result_builder.add_error(
                code=ErrorDetail.Codes.CONNECTION_ERROR,
                message=str(e),
                is_retryable=True,
            )
            return result_builder.build_retry() if task.attempt < task.max_attempts else result_builder.build_failed()

        except Exception as e:
            logger.exception("Task execution failed", task_id=str(task.task_id))
            result_builder.add_error(
                code=ErrorDetail.Codes.UNKNOWN,
                message=str(e),
                is_retryable=False,
            )
            return result_builder.build_failed()

    async def _fetch_page(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        proxy: str | None = None,
    ) -> tuple[str | None, int]:
        """Fetch a page and return HTML content."""
        try:
            async with self._session.get(
                url,
                headers=headers,
                proxy=proxy,
                allow_redirects=True,
                ssl=False,  # For development; enable in production
            ) as response:
                html = await response.text()
                return html, response.status

        except Exception as e:
            logger.error("Fetch failed", url=url, error=str(e))
            return None, 0

    async def _get_schema(
        self,
        schema_id: str,
        version: str = "latest",
    ) -> ParsingSchema | None:
        """Get schema from cache or API."""
        cache_key = f"{schema_id}:{version}"

        if cache_key in self._schemas_cache:
            return self._schemas_cache[cache_key]

        # Fetch from API
        try:
            settings = get_settings()
            api_url = f"http://localhost:{settings.api_port}{settings.api_prefix}/schemas/{schema_id}"

            async with self._session.get(api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    schema = ParsingSchema(**data)
                    self._schemas_cache[cache_key] = schema
                    return schema

        except Exception as e:
            logger.error("Failed to fetch schema", schema_id=schema_id, error=str(e))

        return None

    def _get_proxy(self, profile_id: str | None) -> str | None:
        """Get proxy URL for profile."""
        # Simplified proxy handling - extend for production
        if not profile_id:
            return None

        # Load from configuration or proxy manager
        return None

    def _get_next_page_url(
        self,
        html: str,
        schema: ParsingSchema,
        task: TaskMessage,
    ) -> str | None:
        """Extract next page URL from HTML."""
        from selectolax.parser import HTMLParser
        from urllib.parse import urljoin, urlparse, parse_qs, urlencode

        if not schema.pagination:
            return None

        tree = HTMLParser(html)
        pagination = schema.pagination

        if pagination.type == "next_button" and pagination.selector:
            elements = tree.css(pagination.selector)
            if elements:
                href = elements[0].attributes.get("href")
                if href:
                    return urljoin(task.target_url, href)

        elif pagination.type == "page_param" and pagination.param_name:
            parsed = urlparse(task.target_url)
            params = parse_qs(parsed.query)
            next_page = task.page_number + pagination.param_step
            params[pagination.param_name] = [str(next_page)]
            new_query = urlencode(params, doseq=True)
            return parsed._replace(query=new_query).geturl()

        return None

    async def _create_pagination_task(
        self,
        parent_task: TaskMessage,
        next_url: str,
    ) -> None:
        """Create a task for the next page."""
        child_task = parent_task.child_task(
            target_url=next_url,
            page_number=parent_task.page_number + 1,
        )

        await self._rmq_client.publish_task(
            task=child_task.model_dump(mode="json"),
            mode="http",
        )

        logger.debug(
            "Created pagination task",
            parent_task_id=str(parent_task.task_id),
            child_task_id=str(child_task.task_id),
            page=child_task.page_number,
        )


async def main():
    """Main entry point for HTTP worker."""
    worker = HTTPWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
