"""Data transformation utilities for extracted values."""

import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin


def apply_transformations(value: Any, transformations: list[str], base_url: str = "") -> Any:
    """Apply a list of transformations to a value.

    Args:
        value: The value to transform
        transformations: List of transformation names
        base_url: Base URL for resolving relative URLs

    Returns:
        Transformed value
    """
    if value is None:
        return None

    for transform in transformations:
        value = _apply_single_transform(value, transform, base_url)

    return value


def _apply_single_transform(value: Any, transform: str, base_url: str = "") -> Any:
    """Apply a single transformation."""
    if value is None:
        return None

    # Convert to string if needed for string operations
    str_value = str(value) if not isinstance(value, str) else value

    transform_lower = transform.lower()

    # String transformations
    if transform_lower == "trim":
        return str_value.strip()

    if transform_lower == "lowercase":
        return str_value.lower()

    if transform_lower == "uppercase":
        return str_value.upper()

    if transform_lower == "capitalize":
        return str_value.capitalize()

    if transform_lower == "title":
        return str_value.title()

    # Whitespace normalization
    if transform_lower == "normalize_whitespace":
        return " ".join(str_value.split())

    if transform_lower == "remove_newlines":
        return str_value.replace("\n", " ").replace("\r", "")

    # Number extraction
    if transform_lower == "extract_number":
        return _extract_number(str_value)

    if transform_lower == "extract_int":
        num = _extract_number(str_value)
        return int(num) if num is not None else None

    if transform_lower == "extract_float":
        return _extract_number(str_value)

    # URL handling
    if transform_lower == "absolute_url":
        if base_url and not str_value.startswith(("http://", "https://", "//")):
            return urljoin(base_url, str_value)
        return str_value

    if transform_lower == "extract_domain":
        from urllib.parse import urlparse
        try:
            parsed = urlparse(str_value)
            return parsed.netloc
        except Exception:
            return str_value

    # Date parsing
    if transform_lower == "parse_date":
        return _parse_date(str_value)

    if transform_lower == "parse_datetime":
        return _parse_datetime(str_value)

    # HTML cleaning
    if transform_lower == "strip_html":
        return re.sub(r"<[^>]+>", "", str_value)

    if transform_lower == "decode_entities":
        import html
        return html.unescape(str_value)

    # Currency handling
    if transform_lower == "extract_price":
        return _extract_price(str_value)

    # Boolean conversion
    if transform_lower == "to_bool":
        return _to_bool(str_value)

    # JSON parsing
    if transform_lower == "parse_json":
        import json
        try:
            return json.loads(str_value)
        except Exception:
            return str_value

    # Custom regex (format: regex:pattern:group)
    if transform_lower.startswith("regex:"):
        parts = transform.split(":", 2)
        if len(parts) >= 2:
            pattern = parts[1]
            group = int(parts[2]) if len(parts) > 2 else 0
            return _apply_regex(str_value, pattern, group)

    # Replace (format: replace:old:new)
    if transform_lower.startswith("replace:"):
        parts = transform.split(":", 2)
        if len(parts) >= 3:
            return str_value.replace(parts[1], parts[2])

    # Substring (format: substr:start:end)
    if transform_lower.startswith("substr:"):
        parts = transform.split(":")
        if len(parts) >= 2:
            start = int(parts[1]) if parts[1] else 0
            end = int(parts[2]) if len(parts) > 2 and parts[2] else None
            return str_value[start:end]

    # Default: return as-is
    return value


def _extract_number(value: str) -> float | None:
    """Extract numeric value from string."""
    if not value:
        return None

    # Remove common currency symbols and thousand separators
    cleaned = re.sub(r"[^\d.,\-]", "", value)

    # Handle European format (1.234,56) vs US format (1,234.56)
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            # European format
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # US format
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        # Could be either thousand separator or decimal
        if len(cleaned.split(",")[-1]) == 2:
            # Likely decimal separator
            cleaned = cleaned.replace(",", ".")
        else:
            # Likely thousand separator
            cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_price(value: str) -> dict[str, Any] | None:
    """Extract price with currency from string."""
    if not value:
        return None

    # Common currency patterns
    currency_symbols = {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "¥": "JPY",
        "₽": "RUB",
        "₴": "UAH",
        "zł": "PLN",
        "kr": "SEK",
    }

    currency = None
    for symbol, code in currency_symbols.items():
        if symbol in value:
            currency = code
            break

    amount = _extract_number(value)

    if amount is not None:
        return {"amount": amount, "currency": currency}

    return None


def _parse_date(value: str) -> str | None:
    """Parse date string to ISO format."""
    common_formats = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
    ]

    for fmt in common_formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return value


def _parse_datetime(value: str) -> str | None:
    """Parse datetime string to ISO format."""
    common_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ]

    for fmt in common_formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.isoformat()
        except ValueError:
            continue

    return value


def _to_bool(value: str) -> bool:
    """Convert string to boolean."""
    true_values = {"true", "yes", "1", "on", "да", "есть", "в наличии", "in stock"}
    false_values = {"false", "no", "0", "off", "нет", "отсутствует", "out of stock"}

    lower = value.lower().strip()

    if lower in true_values:
        return True
    if lower in false_values:
        return False

    # Non-empty strings are truthy
    return bool(value.strip())


def _apply_regex(value: str, pattern: str, group: int = 0) -> str | None:
    """Apply regex pattern and return matched group."""
    try:
        match = re.search(pattern, value)
        if match:
            return match.group(group)
    except Exception:
        pass
    return None
