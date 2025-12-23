"""Browser Worker - Playwright-based web scraper for JS-heavy sites."""

import asyncio
import os
import signal
from typing import Any
from uuid import UUID

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import structlog

from src.config import WorkerSettings, get_settings
from src.shared.delta_client import DeltaWriter, TrashSwampWriter
from src.shared.models import (
    ErrorDetail,
    NavigationStep,
    ParsingSchema,
    TaskMessage,
)
from src.shared.rmq_client import RabbitMQClient
from src.uca.common import DataExtractor, ResultBuilder

logger = structlog.get_logger()


class BrowserWorker:
    """Playwright-based browser worker for JavaScript-rendered pages."""

    def __init__(
        self,
        worker_id: str | None = None,
        settings: WorkerSettings | None = None,
    ):
        self.settings = settings or get_settings().worker
        self.worker_id = worker_id or f"browser-worker-{os.getpid()}"

        self._rmq_client: RabbitMQClient | None = None
        self._browser: Browser | None = None
        self._playwright = None
        self._delta_writer: DeltaWriter | None = None
        self._trash_writer: TrashSwampWriter | None = None
        self._schemas_cache: dict[str, ParsingSchema] = {}
        self._running = False
        self._tasks_processed = 0
        self._context_pool: list[BrowserContext] = []

    async def start(self) -> None:
        """Start the browser worker."""
        logger.info("Starting Browser Worker", worker_id=self.worker_id)

        # Initialize Playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        # Create context pool
        for _ in range(self.settings.browser_sessions):
            context = await self._create_context()
            self._context_pool.append(context)

        # Initialize other clients
        self._rmq_client = RabbitMQClient()
        await self._rmq_client.connect()

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
            queue_name="tasks.browser",
            callback=self._process_message,
            prefetch_count=self.settings.browser_prefetch,
        )

        logger.info("Browser Worker started", worker_id=self.worker_id)

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        logger.info("Stopping Browser Worker", worker_id=self.worker_id)
        self._running = False

        # Close contexts
        for context in self._context_pool:
            await context.close()

        if self._browser:
            await self._browser.close()

        if self._playwright:
            await self._playwright.stop()

        if self._rmq_client:
            await self._rmq_client.close()

        logger.info(
            "Browser Worker stopped",
            worker_id=self.worker_id,
            tasks_processed=self._tasks_processed,
        )

    async def _create_context(self) -> BrowserContext:
        """Create a new browser context with stealth settings."""
        context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
            permissions=["geolocation"],
            geolocation={"latitude": 40.7128, "longitude": -74.0060},
            java_script_enabled=True,
        )

        # Add stealth scripts
        await context.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)

        return context

    async def _get_context(self) -> BrowserContext:
        """Get a context from the pool."""
        if self._context_pool:
            return self._context_pool.pop()
        return await self._create_context()

    async def _return_context(self, context: BrowserContext) -> None:
        """Return a context to the pool."""
        # Clear cookies and storage
        await context.clear_cookies()
        self._context_pool.append(context)

    async def _process_message(self, message) -> None:
        """Process a single task message."""
        async with message.process():
            try:
                task = TaskMessage.model_validate_json(message.body)
                logger.info(
                    "Processing browser task",
                    task_id=str(task.task_id),
                    url=task.target_url,
                )

                result = await self._execute_task(task)

                await self._rmq_client.publish_result(
                    result.model_dump(mode="json")
                )

                self._tasks_processed += 1

            except Exception as e:
                logger.exception("Failed to process message", error=str(e))

    async def _execute_task(self, task: TaskMessage) -> Any:
        """Execute a browser scraping task."""
        result_builder = ResultBuilder(task.task_id, task.run_id)
        result_builder.set_started()
        result_builder.set_worker_id(self.worker_id)

        context = await self._get_context()
        page: Page | None = None

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

            # Create page
            page = await context.new_page()

            # Set cookies if provided
            if task.cookies:
                await context.add_cookies(task.cookies)

            # Navigate to URL
            response = await page.goto(
                task.target_url,
                wait_until="networkidle",
                timeout=self.settings.request_timeout * 1000,
            )

            result_builder.set_http_status(response.status if response else 0)
            result_builder.increment_requests()

            if not response or response.status >= 400:
                result_builder.add_error(
                    code=ErrorDetail.Codes.HTTP_ERROR,
                    message=f"HTTP {response.status if response else 'no response'}",
                    is_retryable=response and response.status in (429, 500, 502, 503, 504),
                )
                return result_builder.build_failed()

            # Execute navigation steps
            if schema.navigation_steps:
                await self._execute_navigation(page, schema.navigation_steps)

            # Handle infinite scroll if needed
            if schema.pagination and schema.pagination.type == "infinite_scroll":
                await self._handle_infinite_scroll(page, schema)

            # Get HTML content
            html = await page.content()
            result_builder.add_bytes_downloaded(len(html))

            # Take screenshot for debugging
            screenshot = await page.screenshot()
            debug_paths = self._trash_writer.write_debug(
                task_id=str(task.task_id),
                html=html,
                screenshot=screenshot,
            )
            if debug_paths.get("screenshot"):
                result_builder.set_screenshot_path(debug_paths["screenshot"])
            if debug_paths.get("html"):
                result_builder.set_raw_html_path(debug_paths["html"])

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

            # Check for next page
            if schema.pagination and schema.pagination.type in ("next_button", "load_more"):
                next_url = await self._get_next_page_url(page, schema, task)
                if next_url:
                    result_builder.set_pagination(
                        has_next=True,
                        next_url=next_url,
                        current_page=task.page_number,
                    )
                    await self._create_pagination_task(task, next_url)

            return result_builder.build_success()

        except asyncio.TimeoutError:
            result_builder.add_error(
                code=ErrorDetail.Codes.TIMEOUT,
                message="Page load timeout",
                is_retryable=True,
            )
            return result_builder.build_retry() if task.attempt < task.max_attempts else result_builder.build_failed()

        except Exception as e:
            logger.exception("Browser task failed", task_id=str(task.task_id))
            result_builder.add_error(
                code=ErrorDetail.Codes.UNKNOWN,
                message=str(e),
                is_retryable=False,
            )
            return result_builder.build_failed()

        finally:
            if page:
                await page.close()
            await self._return_context(context)

    async def _execute_navigation(
        self,
        page: Page,
        steps: list[NavigationStep],
    ) -> None:
        """Execute navigation steps on the page."""
        for step in steps:
            try:
                logger.debug("Executing navigation step", action=step.action, target=step.target)

                if step.action == "click":
                    await page.click(step.target, timeout=10000)

                elif step.action == "input":
                    await page.fill(step.target, step.value or "")

                elif step.action == "scroll":
                    if step.target:
                        await page.eval_on_selector(
                            step.target,
                            "el => el.scrollIntoView()"
                        )
                    else:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

                elif step.action == "wait":
                    if step.target:
                        await page.wait_for_selector(step.target, timeout=10000)
                    else:
                        await asyncio.sleep(step.wait_ms / 1000)

                elif step.action == "hover":
                    await page.hover(step.target)

                elif step.action == "select":
                    await page.select_option(step.target, step.value)

                elif step.action == "screenshot":
                    pass  # Handled separately

                # Wait after action if specified
                if step.wait_ms > 0:
                    await asyncio.sleep(step.wait_ms / 1000)

                if step.wait_for:
                    await page.wait_for_selector(step.wait_for, timeout=10000)

            except Exception as e:
                if not step.optional:
                    raise
                logger.warning(
                    "Optional navigation step failed",
                    action=step.action,
                    error=str(e),
                )

    async def _handle_infinite_scroll(
        self,
        page: Page,
        schema: ParsingSchema,
    ) -> None:
        """Handle infinite scroll pagination."""
        if not schema.pagination:
            return

        delay = schema.pagination.scroll_delay_ms / 1000
        max_scrolls = schema.pagination.max_pages

        previous_height = 0
        scroll_count = 0

        while scroll_count < max_scrolls:
            # Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(delay)

            # Check if new content loaded
            new_height = await page.evaluate("document.body.scrollHeight")

            if new_height == previous_height:
                # Check for stop selector
                if schema.pagination.stop_selector:
                    try:
                        await page.wait_for_selector(
                            schema.pagination.stop_selector,
                            timeout=1000,
                        )
                        break
                    except Exception:
                        pass
                break

            previous_height = new_height
            scroll_count += 1

        logger.debug("Infinite scroll completed", scrolls=scroll_count)

    async def _get_next_page_url(
        self,
        page: Page,
        schema: ParsingSchema,
        task: TaskMessage,
    ) -> str | None:
        """Get next page URL from current page."""
        if not schema.pagination or not schema.pagination.selector:
            return None

        try:
            element = await page.query_selector(schema.pagination.selector)
            if element:
                href = await element.get_attribute("href")
                if href:
                    return page.url if href.startswith("javascript:") else href
        except Exception as e:
            logger.debug("Failed to get next page URL", error=str(e))

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
            mode="browser",
        )

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
        import aiohttp

        try:
            settings = get_settings()
            api_url = f"http://localhost:{settings.api_port}{settings.api_prefix}/schemas/{schema_id}"

            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        schema = ParsingSchema(**data)
                        self._schemas_cache[cache_key] = schema
                        return schema

        except Exception as e:
            logger.error("Failed to fetch schema", schema_id=schema_id, error=str(e))

        return None


async def main():
    """Main entry point for browser worker."""
    worker = BrowserWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
