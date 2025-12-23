"""
Unit tests for data transformers.
"""
import pytest

from src.uca.common.transformers import (
    apply_transformations,
    _apply_single_transform,
    _extract_number,
    _extract_price,
    _parse_date,
    _parse_datetime,
    _to_bool,
    _apply_regex,
)


class TestApplyTransformations:
    """Tests for apply_transformations function."""

    def test_none_value(self):
        """Test that None values are returned as-is."""
        result = apply_transformations(None, ["trim", "lowercase"])
        assert result is None

    def test_empty_transformations(self):
        """Test with empty transformation list."""
        result = apply_transformations("Hello World", [])
        assert result == "Hello World"

    def test_single_transformation(self):
        """Test applying a single transformation."""
        result = apply_transformations("  Hello  ", ["trim"])
        assert result == "Hello"

    def test_multiple_transformations(self):
        """Test applying multiple transformations in order."""
        result = apply_transformations("  HELLO WORLD  ", ["trim", "lowercase"])
        assert result == "hello world"

    def test_transformation_chain(self):
        """Test a complex transformation chain."""
        result = apply_transformations(
            "  $1,234.56  ",
            ["trim", "extract_number"]
        )
        assert result == 1234.56


class TestStringTransformations:
    """Tests for string transformation functions."""

    def test_trim(self):
        """Test trim transformation."""
        assert _apply_single_transform("  hello  ", "trim") == "hello"
        assert _apply_single_transform("\n\thello\n\t", "trim") == "hello"

    def test_lowercase(self):
        """Test lowercase transformation."""
        assert _apply_single_transform("HELLO", "lowercase") == "hello"
        assert _apply_single_transform("HeLLo WoRLd", "lowercase") == "hello world"

    def test_uppercase(self):
        """Test uppercase transformation."""
        assert _apply_single_transform("hello", "uppercase") == "HELLO"
        assert _apply_single_transform("Hello World", "uppercase") == "HELLO WORLD"

    def test_capitalize(self):
        """Test capitalize transformation."""
        assert _apply_single_transform("hello", "capitalize") == "Hello"
        assert _apply_single_transform("HELLO", "capitalize") == "Hello"

    def test_title(self):
        """Test title transformation."""
        assert _apply_single_transform("hello world", "title") == "Hello World"

    def test_normalize_whitespace(self):
        """Test whitespace normalization."""
        result = _apply_single_transform("hello   world\n\tfoo", "normalize_whitespace")
        assert result == "hello world foo"

    def test_remove_newlines(self):
        """Test newline removal."""
        result = _apply_single_transform("hello\nworld\r\n", "remove_newlines")
        assert result == "hello world "


class TestExtractNumber:
    """Tests for number extraction."""

    def test_simple_integer(self):
        """Test extracting simple integers."""
        assert _extract_number("123") == 123.0
        assert _extract_number("  456  ") == 456.0

    def test_simple_float(self):
        """Test extracting simple floats."""
        assert _extract_number("123.45") == 123.45
        assert _extract_number("0.5") == 0.5

    def test_with_currency_symbol(self):
        """Test extracting numbers with currency symbols."""
        assert _extract_number("$29.99") == 29.99
        assert _extract_number("€100.00") == 100.0
        assert _extract_number("£50") == 50.0

    def test_us_format_with_thousands(self):
        """Test US format with thousand separators."""
        assert _extract_number("1,234.56") == 1234.56
        assert _extract_number("$1,000,000.00") == 1000000.0

    def test_european_format(self):
        """Test European format with thousand separators."""
        assert _extract_number("1.234,56") == 1234.56

    def test_negative_numbers(self):
        """Test extracting negative numbers."""
        assert _extract_number("-123.45") == -123.45
        assert _extract_number("-$50.00") == -50.0

    def test_empty_string(self):
        """Test with empty string."""
        assert _extract_number("") is None

    def test_no_numbers(self):
        """Test with string containing no numbers."""
        assert _extract_number("no numbers here") is None


class TestExtractPrice:
    """Tests for price extraction."""

    def test_usd_price(self):
        """Test extracting USD price."""
        result = _extract_price("$29.99")
        assert result == {"amount": 29.99, "currency": "USD"}

    def test_eur_price(self):
        """Test extracting EUR price."""
        result = _extract_price("€100,50")
        assert result == {"amount": 100.50, "currency": "EUR"}

    def test_gbp_price(self):
        """Test extracting GBP price."""
        result = _extract_price("£75.00")
        assert result == {"amount": 75.0, "currency": "GBP"}

    def test_rub_price(self):
        """Test extracting RUB price."""
        result = _extract_price("1500 ₽")
        assert result == {"amount": 1500.0, "currency": "RUB"}

    def test_price_without_currency(self):
        """Test extracting price without recognized currency."""
        result = _extract_price("50.00")
        assert result == {"amount": 50.0, "currency": None}

    def test_empty_price(self):
        """Test with empty string."""
        assert _extract_price("") is None

    def test_invalid_price(self):
        """Test with invalid price string."""
        assert _extract_price("not a price") is None


class TestDateParsing:
    """Tests for date parsing."""

    def test_iso_format(self):
        """Test ISO date format."""
        assert _parse_date("2024-01-15") == "2024-01-15"

    def test_european_format(self):
        """Test European date format (dd.mm.yyyy)."""
        assert _parse_date("15.01.2024") == "2024-01-15"

    def test_european_slash_format(self):
        """Test European slash format (dd/mm/yyyy)."""
        assert _parse_date("15/01/2024") == "2024-01-15"

    def test_us_format(self):
        """Test US date format (mm/dd/yyyy)."""
        # Note: ambiguous dates may not parse correctly
        assert _parse_date("01/15/2024") == "2024-01-15"

    def test_long_format(self):
        """Test long date format."""
        assert _parse_date("January 15, 2024") == "2024-01-15"

    def test_short_month_format(self):
        """Test short month format."""
        assert _parse_date("Jan 15, 2024") == "2024-01-15"

    def test_invalid_date(self):
        """Test invalid date returns original value."""
        result = _parse_date("not a date")
        assert result == "not a date"


class TestDatetimeParsing:
    """Tests for datetime parsing."""

    def test_iso_datetime(self):
        """Test ISO datetime format."""
        result = _parse_datetime("2024-01-15T10:30:00")
        assert result == "2024-01-15T10:30:00"

    def test_iso_datetime_with_z(self):
        """Test ISO datetime with Z suffix."""
        result = _parse_datetime("2024-01-15T10:30:00Z")
        assert result == "2024-01-15T10:30:00"

    def test_space_separated(self):
        """Test space-separated datetime."""
        result = _parse_datetime("2024-01-15 10:30:00")
        assert result == "2024-01-15T10:30:00"

    def test_invalid_datetime(self):
        """Test invalid datetime returns original value."""
        result = _parse_datetime("not a datetime")
        assert result == "not a datetime"


class TestToBool:
    """Tests for boolean conversion."""

    def test_true_values(self):
        """Test strings that should convert to True."""
        assert _to_bool("true") is True
        assert _to_bool("True") is True
        assert _to_bool("TRUE") is True
        assert _to_bool("yes") is True
        assert _to_bool("1") is True
        assert _to_bool("on") is True

    def test_russian_true_values(self):
        """Test Russian strings that should convert to True."""
        assert _to_bool("да") is True
        assert _to_bool("есть") is True
        assert _to_bool("в наличии") is True

    def test_false_values(self):
        """Test strings that should convert to False."""
        assert _to_bool("false") is False
        assert _to_bool("False") is False
        assert _to_bool("no") is False
        assert _to_bool("0") is False
        assert _to_bool("off") is False

    def test_russian_false_values(self):
        """Test Russian strings that should convert to False."""
        assert _to_bool("нет") is False
        assert _to_bool("отсутствует") is False

    def test_non_empty_truthy(self):
        """Test that non-empty strings are truthy."""
        assert _to_bool("something") is True
        assert _to_bool("hello") is True

    def test_empty_falsy(self):
        """Test that empty strings are falsy."""
        assert _to_bool("") is False
        assert _to_bool("   ") is False


class TestHtmlTransformations:
    """Tests for HTML-related transformations."""

    def test_strip_html(self):
        """Test stripping HTML tags."""
        result = _apply_single_transform(
            "<p>Hello <b>World</b></p>",
            "strip_html"
        )
        assert result == "Hello World"

    def test_strip_html_complex(self):
        """Test stripping complex HTML."""
        result = _apply_single_transform(
            '<div class="foo"><a href="bar">Link</a></div>',
            "strip_html"
        )
        assert result == "Link"

    def test_decode_entities(self):
        """Test decoding HTML entities."""
        result = _apply_single_transform(
            "&lt;hello&gt; &amp; &quot;world&quot;",
            "decode_entities"
        )
        assert result == '<hello> & "world"'


class TestUrlTransformations:
    """Tests for URL transformations."""

    def test_absolute_url_relative(self):
        """Test converting relative URL to absolute."""
        result = _apply_single_transform(
            "/product/123",
            "absolute_url",
            "https://example.com"
        )
        assert result == "https://example.com/product/123"

    def test_absolute_url_already_absolute(self):
        """Test that absolute URLs are unchanged."""
        result = _apply_single_transform(
            "https://other.com/page",
            "absolute_url",
            "https://example.com"
        )
        assert result == "https://other.com/page"

    def test_extract_domain(self):
        """Test extracting domain from URL."""
        result = _apply_single_transform(
            "https://www.example.com/path/to/page",
            "extract_domain"
        )
        assert result == "www.example.com"


class TestRegexTransformations:
    """Tests for regex transformations."""

    def test_regex_extraction(self):
        """Test regex pattern extraction."""
        result = _apply_regex("Product ID: ABC123", r"ID:\s*(\w+)", 1)
        assert result == "ABC123"

    def test_regex_no_group(self):
        """Test regex extraction with default group."""
        result = _apply_regex("Price: $99.99", r"\$[\d.]+", 0)
        assert result == "$99.99"

    def test_regex_no_match(self):
        """Test regex with no match."""
        result = _apply_regex("no numbers", r"\d+", 0)
        assert result is None

    def test_regex_transformation_via_apply(self):
        """Test regex transformation through apply function."""
        result = _apply_single_transform(
            "SKU: PRD-12345",
            "regex:SKU:\\s*(\\S+):1"
        )
        assert result == "PRD-12345"


class TestReplaceTransformation:
    """Tests for replace transformation."""

    def test_simple_replace(self):
        """Test simple string replacement."""
        result = _apply_single_transform(
            "Hello World",
            "replace:World:Universe"
        )
        assert result == "Hello Universe"

    def test_replace_multiple_occurrences(self):
        """Test replacing multiple occurrences."""
        result = _apply_single_transform(
            "a-b-c-d",
            "replace:-:_"
        )
        assert result == "a_b_c_d"


class TestSubstrTransformation:
    """Tests for substring transformation."""

    def test_substr_start_only(self):
        """Test substring with start index only."""
        result = _apply_single_transform("Hello World", "substr:6")
        assert result == "World"

    def test_substr_start_and_end(self):
        """Test substring with start and end indices."""
        result = _apply_single_transform("Hello World", "substr:0:5")
        assert result == "Hello"

    def test_substr_negative(self):
        """Test substring with negative index."""
        result = _apply_single_transform("Hello World", "substr:-5")
        assert result == "World"


class TestJsonTransformation:
    """Tests for JSON transformation."""

    def test_parse_valid_json(self):
        """Test parsing valid JSON."""
        result = _apply_single_transform(
            '{"key": "value", "num": 123}',
            "parse_json"
        )
        assert result == {"key": "value", "num": 123}

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON returns original."""
        result = _apply_single_transform("not json", "parse_json")
        assert result == "not json"

    def test_parse_json_array(self):
        """Test parsing JSON array."""
        result = _apply_single_transform('[1, 2, 3]', "parse_json")
        assert result == [1, 2, 3]


class TestUnknownTransformation:
    """Tests for unknown transformations."""

    def test_unknown_transformation(self):
        """Test that unknown transformations return value unchanged."""
        result = _apply_single_transform("hello", "unknown_transform")
        assert result == "hello"
