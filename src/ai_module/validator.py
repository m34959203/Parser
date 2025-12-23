"""Schema validation and testing."""

from dataclasses import dataclass, field
from typing import Any

from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser
import structlog

from src.shared.models import ParsingSchema, FieldDefinition, ExtractionMethod

logger = structlog.get_logger()


@dataclass
class FieldResult:
    """Result for a single field extraction."""
    field_name: str
    success: bool
    value: Any = None
    elements_count: int = 0
    error: str | None = None


@dataclass
class TestResult:
    """Result of schema validation."""
    success: bool
    records_found: int = 0
    fields_extracted: dict[str, int] = field(default_factory=dict)
    field_results: list[FieldResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Extended validation result."""
    success: bool
    records_found: int = 0
    fields_extracted: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class SchemaValidator:
    """Validator for parsing schemas."""

    async def validate_schema(
        self,
        schema: ParsingSchema,
        url: str,
    ) -> TestResult:
        """Validate a schema against a URL.

        Args:
            schema: Parsing schema to validate
            url: URL to test against

        Returns:
            Test results with field-level details
        """
        logger.info("Validating schema", schema_id=schema.schema_id, url=url)

        # Fetch page
        html = await self._fetch_page(url, schema.requires_js)

        if not html:
            return TestResult(
                success=False,
                errors=["Failed to fetch page"],
            )

        # Parse HTML
        tree = HTMLParser(html)

        # Test each field
        field_results = []
        fields_extracted = {}
        errors = []
        suggestions = []

        for field_def in schema.fields:
            result = self._test_field(tree, field_def)
            field_results.append(result)

            if result.success:
                fields_extracted[result.field_name] = result.elements_count
            else:
                errors.append(f"Field '{result.field_name}': {result.error}")

                # Add suggestions
                suggestion = self._suggest_fix(tree, field_def)
                if suggestion:
                    suggestions.append(f"{result.field_name}: {suggestion}")

        # Check for container
        records_found = 1
        if schema.item_container:
            containers = tree.css(schema.item_container)
            records_found = len(containers)

            if records_found == 0:
                errors.append(f"Container '{schema.item_container}' not found")

        success = len(errors) == 0 and records_found > 0

        return TestResult(
            success=success,
            records_found=records_found,
            fields_extracted=fields_extracted,
            field_results=field_results,
            errors=errors,
            suggestions=suggestions,
        )

    async def _fetch_page(self, url: str, requires_js: bool) -> str | None:
        """Fetch page content."""
        if requires_js:
            return await self._fetch_with_playwright(url)
        else:
            return await self._fetch_with_http(url)

    async def _fetch_with_http(self, url: str) -> str | None:
        """Fetch page using aiohttp."""
        import aiohttp

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 200:
                        return await response.text()
        except Exception as e:
            logger.error("HTTP fetch failed", url=url, error=str(e))

        return None

    async def _fetch_with_playwright(self, url: str) -> str | None:
        """Fetch page using Playwright for JS rendering."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            try:
                page = await browser.new_page()
                await page.goto(url, wait_until="networkidle", timeout=30000)
                return await page.content()
            except Exception as e:
                logger.error("Playwright fetch failed", url=url, error=str(e))
                return None
            finally:
                await browser.close()

    def _test_field(
        self,
        tree: HTMLParser,
        field_def: FieldDefinition,
    ) -> FieldResult:
        """Test a single field extraction."""
        try:
            if field_def.method == ExtractionMethod.CSS:
                return self._test_css_field(tree, field_def)
            elif field_def.method == ExtractionMethod.XPATH:
                return self._test_xpath_field(tree, field_def)
            elif field_def.method == ExtractionMethod.REGEX:
                return self._test_regex_field(tree, field_def)
            else:
                return FieldResult(
                    field_name=field_def.name,
                    success=False,
                    error=f"Unsupported method: {field_def.method}",
                )
        except Exception as e:
            return FieldResult(
                field_name=field_def.name,
                success=False,
                error=str(e),
            )

    def _test_css_field(
        self,
        tree: HTMLParser,
        field_def: FieldDefinition,
    ) -> FieldResult:
        """Test CSS selector extraction."""
        # Handle attribute in selector
        selector = field_def.selector
        attribute = field_def.attribute

        if "@" in selector and not attribute:
            selector, attribute = selector.rsplit("@", 1)

        elements = tree.css(selector)

        if not elements:
            # Try fallback selectors
            for fallback in field_def.fallback_selectors:
                elements = tree.css(fallback)
                if elements:
                    break

        if not elements:
            return FieldResult(
                field_name=field_def.name,
                success=False,
                error=f"Selector '{field_def.selector}' found 0 elements",
            )

        # Get first element's value
        element = elements[0]

        if attribute:
            value = element.attributes.get(attribute)
        else:
            value = element.text(deep=True, strip=True)

        # Validate value based on type
        if field_def.required and not value:
            return FieldResult(
                field_name=field_def.name,
                success=False,
                elements_count=len(elements),
                error="Required field has empty value",
            )

        return FieldResult(
            field_name=field_def.name,
            success=True,
            value=value,
            elements_count=len(elements),
        )

    def _test_xpath_field(
        self,
        tree: HTMLParser,
        field_def: FieldDefinition,
    ) -> FieldResult:
        """Test XPath extraction."""
        from lxml import html as lxml_html

        try:
            lxml_tree = lxml_html.fromstring(tree.html)
            results = lxml_tree.xpath(field_def.selector)

            if not results:
                return FieldResult(
                    field_name=field_def.name,
                    success=False,
                    error=f"XPath '{field_def.selector}' found 0 elements",
                )

            result = results[0]

            if isinstance(result, str):
                value = result
            elif field_def.attribute:
                value = result.get(field_def.attribute)
            else:
                value = result.text_content().strip() if hasattr(result, 'text_content') else str(result)

            return FieldResult(
                field_name=field_def.name,
                success=True,
                value=value,
                elements_count=len(results),
            )

        except Exception as e:
            return FieldResult(
                field_name=field_def.name,
                success=False,
                error=f"XPath error: {str(e)}",
            )

    def _test_regex_field(
        self,
        tree: HTMLParser,
        field_def: FieldDefinition,
    ) -> FieldResult:
        """Test regex extraction."""
        import re

        try:
            match = re.search(field_def.selector, tree.html, re.DOTALL)

            if not match:
                return FieldResult(
                    field_name=field_def.name,
                    success=False,
                    error=f"Regex '{field_def.selector}' found no matches",
                )

            value = match.group(1) if match.groups() else match.group(0)

            return FieldResult(
                field_name=field_def.name,
                success=True,
                value=value,
                elements_count=1,
            )

        except Exception as e:
            return FieldResult(
                field_name=field_def.name,
                success=False,
                error=f"Regex error: {str(e)}",
            )

    def _suggest_fix(
        self,
        tree: HTMLParser,
        field_def: FieldDefinition,
    ) -> str | None:
        """Suggest fixes for a failed field."""
        # Try to find similar selectors
        selector_parts = field_def.selector.split()

        if len(selector_parts) > 1:
            # Try simpler selector
            simple_selector = selector_parts[-1]
            elements = tree.css(simple_selector)
            if elements:
                return f"Try simpler selector: '{simple_selector}' (found {len(elements)} elements)"

        # Try common patterns
        name_lower = field_def.name.lower()
        common_patterns = {
            "title": ["h1", "h2", ".title", ".name", "[class*='title']"],
            "price": [".price", "[class*='price']", "span.price", ".cost"],
            "image": ["img", "img.product", "[class*='image'] img"],
            "url": ["a", "a.link", "[class*='link']"],
        }

        for key, patterns in common_patterns.items():
            if key in name_lower:
                for pattern in patterns:
                    elements = tree.css(pattern)
                    if elements:
                        return f"Try common selector: '{pattern}' (found {len(elements)} elements)"

        return None
