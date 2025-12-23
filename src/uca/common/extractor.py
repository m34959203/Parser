"""Data extraction from HTML using parsing schemas."""

import re
from typing import Any

from selectolax.parser import HTMLParser
import structlog

from src.shared.models import ExtractionMethod, FieldDefinition, FieldType, ParsingSchema
from .transformers import apply_transformations

logger = structlog.get_logger()


class DataExtractor:
    """Extract data from HTML using a parsing schema."""

    def __init__(self, schema: ParsingSchema, base_url: str = ""):
        self.schema = schema
        self.base_url = base_url

    def extract(self, html: str) -> list[dict[str, Any]]:
        """Extract records from HTML.

        Args:
            html: HTML content to parse

        Returns:
            List of extracted records
        """
        tree = HTMLParser(html)
        records = []

        if self.schema.item_container:
            # Extract multiple items from container
            containers = tree.css(self.schema.item_container)
            logger.debug(
                "Found containers",
                selector=self.schema.item_container,
                count=len(containers),
            )

            for container in containers:
                record = self._extract_record(container)
                if self._validate_record(record):
                    records.append(record)
        else:
            # Single page extraction
            record = self._extract_record(tree)
            if self._validate_record(record):
                records.append(record)

        logger.info(
            "Extraction complete",
            total_found=len(records) + (len(containers) - len(records) if self.schema.item_container else 0),
            valid_records=len(records),
        )

        return records

    def _extract_record(self, node: HTMLParser) -> dict[str, Any]:
        """Extract a single record from a node."""
        record = {}

        for field in self.schema.fields:
            value = self._extract_field(node, field)

            if value is not None:
                # Apply transformations
                value = apply_transformations(
                    value,
                    field.transformations,
                    self.base_url,
                )

                # Type conversion
                value = self._convert_type(value, field.type)

                # Validation
                if field.validation_regex and value:
                    if not re.match(field.validation_regex, str(value)):
                        logger.debug(
                            "Field failed validation",
                            field=field.name,
                            value=value,
                            pattern=field.validation_regex,
                        )
                        value = field.default

            # Use default if no value
            if value is None and field.default is not None:
                value = field.default

            record[field.name] = value

        return record

    def _extract_field(self, node: HTMLParser, field: FieldDefinition) -> Any:
        """Extract a single field value."""
        # Try primary selector
        value = self._extract_with_selector(node, field.method, field.selector, field.attribute)

        # Try fallback selectors if needed
        if value is None and field.fallback_selectors:
            for fallback in field.fallback_selectors:
                value = self._extract_with_selector(node, field.method, fallback, field.attribute)
                if value is not None:
                    break

        return value

    def _extract_with_selector(
        self,
        node: HTMLParser,
        method: ExtractionMethod,
        selector: str,
        attribute: str | None = None,
    ) -> Any:
        """Extract value using specified method and selector."""
        try:
            if method == ExtractionMethod.CSS:
                return self._extract_css(node, selector, attribute)
            elif method == ExtractionMethod.XPATH:
                return self._extract_xpath(node, selector, attribute)
            elif method == ExtractionMethod.REGEX:
                return self._extract_regex(node, selector)
            elif method == ExtractionMethod.JSON_PATH:
                return self._extract_jsonpath(node, selector)
        except Exception as e:
            logger.debug("Extraction failed", selector=selector, error=str(e))

        return None

    def _extract_css(
        self,
        node: HTMLParser,
        selector: str,
        attribute: str | None = None,
    ) -> Any:
        """Extract using CSS selector."""
        # Handle attribute in selector (e.g., "img@src")
        if "@" in selector and not attribute:
            selector, attribute = selector.rsplit("@", 1)

        elements = node.css(selector)

        if not elements:
            return None

        element = elements[0]

        if attribute:
            return element.attributes.get(attribute)

        return element.text(deep=True, strip=True)

    def _extract_xpath(
        self,
        node: HTMLParser,
        selector: str,
        attribute: str | None = None,
    ) -> Any:
        """Extract using XPath.

        Note: selectolax doesn't support XPath directly,
        so we convert simple XPath to CSS or use lxml.
        """
        from lxml import html as lxml_html

        try:
            # Parse with lxml for XPath support
            tree = lxml_html.fromstring(node.html)
            results = tree.xpath(selector)

            if not results:
                return None

            result = results[0]

            if isinstance(result, str):
                return result

            if attribute:
                return result.get(attribute)

            return result.text_content().strip() if hasattr(result, 'text_content') else str(result)

        except Exception as e:
            logger.debug("XPath extraction failed", selector=selector, error=str(e))
            return None

    def _extract_regex(self, node: HTMLParser, pattern: str) -> Any:
        """Extract using regex pattern."""
        html = node.html if hasattr(node, 'html') else str(node)

        match = re.search(pattern, html, re.DOTALL)
        if match:
            # Return first group if exists, else whole match
            return match.group(1) if match.groups() else match.group(0)

        return None

    def _extract_jsonpath(self, node: HTMLParser, path: str) -> Any:
        """Extract from embedded JSON using JSONPath."""
        import json

        # Try to find JSON in script tags
        scripts = node.css("script[type='application/json'], script[type='application/ld+json']")

        for script in scripts:
            try:
                data = json.loads(script.text())

                # Simple JSONPath implementation
                value = self._get_jsonpath_value(data, path)
                if value is not None:
                    return value

            except json.JSONDecodeError:
                continue

        return None

    def _get_jsonpath_value(self, data: Any, path: str) -> Any:
        """Simple JSONPath value getter."""
        # Remove leading $. if present
        path = path.lstrip("$").lstrip(".")

        parts = path.split(".")
        current = data

        for part in parts:
            if not part:
                continue

            # Handle array index
            if "[" in part:
                key, index = part.rstrip("]").split("[")
                if key:
                    current = current.get(key) if isinstance(current, dict) else None
                if current and isinstance(current, list):
                    try:
                        current = current[int(index)]
                    except (IndexError, ValueError):
                        return None
            else:
                current = current.get(part) if isinstance(current, dict) else None

            if current is None:
                return None

        return current

    def _convert_type(self, value: Any, field_type: FieldType) -> Any:
        """Convert value to the specified type."""
        if value is None:
            return None

        try:
            if field_type == FieldType.STRING:
                return str(value)

            if field_type == FieldType.INTEGER:
                if isinstance(value, (int, float)):
                    return int(value)
                return int(float(str(value).replace(",", "").replace(" ", "")))

            if field_type == FieldType.FLOAT:
                if isinstance(value, (int, float)):
                    return float(value)
                return float(str(value).replace(",", ".").replace(" ", ""))

            if field_type == FieldType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ("true", "yes", "1", "да")

            if field_type == FieldType.URL:
                return str(value)

            if field_type == FieldType.DATETIME:
                return str(value)

            if field_type == FieldType.LIST:
                if isinstance(value, list):
                    return value
                return [value]

            if field_type == FieldType.JSON:
                import json
                if isinstance(value, (dict, list)):
                    return value
                return json.loads(str(value))

        except (ValueError, TypeError) as e:
            logger.debug(
                "Type conversion failed",
                value=value,
                target_type=field_type,
                error=str(e),
            )

        return value

    def _validate_record(self, record: dict[str, Any]) -> bool:
        """Validate extracted record meets minimum requirements."""
        required_fields = [f for f in self.schema.fields if f.required]
        filled_fields = sum(1 for f in required_fields if record.get(f.name) is not None)

        # Check minimum fields requirement
        if filled_fields < self.schema.min_fields_required:
            return False

        # Check all required fields
        for field in required_fields:
            if record.get(field.name) is None:
                logger.debug("Required field missing", field=field.name)
                return False

        return True
