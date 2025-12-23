"""
Unit tests for DataExtractor.
"""
import pytest
from unittest.mock import patch, MagicMock

from src.shared.models import (
    ParsingSchema,
    FieldDefinition,
    FieldType,
    ExtractionMethod,
)
from src.uca.common.extractor import DataExtractor


@pytest.fixture
def simple_html():
    """Simple HTML for testing."""
    return """
    <html>
    <head><title>Test Page</title></head>
    <body>
        <div class="product">
            <h1 class="title">Test Product</h1>
            <span class="price" data-value="29.99">$29.99</span>
            <p class="description">Product description here.</p>
            <a href="/product/123" class="link">View Details</a>
            <img src="/images/product.jpg" alt="Product Image" />
        </div>
    </body>
    </html>
    """


@pytest.fixture
def list_html():
    """HTML with multiple items for testing."""
    return """
    <html>
    <body>
        <div class="product-list">
            <div class="product-card">
                <h2 class="name">Product One</h2>
                <span class="price">$19.99</span>
            </div>
            <div class="product-card">
                <h2 class="name">Product Two</h2>
                <span class="price">$29.99</span>
            </div>
            <div class="product-card">
                <h2 class="name">Product Three</h2>
                <span class="price">$39.99</span>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def json_html():
    """HTML with embedded JSON."""
    return """
    <html>
    <body>
        <script type="application/ld+json">
        {
            "@type": "Product",
            "name": "JSON Product",
            "offers": {
                "price": 49.99,
                "currency": "USD"
            }
        }
        </script>
        <div class="content">Regular content</div>
    </body>
    </html>
    """


@pytest.fixture
def simple_schema():
    """Simple schema for single page extraction."""
    return ParsingSchema(
        schema_id="test_single",
        source_id="test",
        start_url="https://test.com",
        fields=[
            FieldDefinition(
                name="title",
                selector="h1.title",
                method=ExtractionMethod.CSS,
            ),
            FieldDefinition(
                name="price",
                selector="span.price",
                attribute="data-value",
                type=FieldType.FLOAT,
                method=ExtractionMethod.CSS,
            ),
            FieldDefinition(
                name="description",
                selector="p.description",
                method=ExtractionMethod.CSS,
            ),
        ],
    )


@pytest.fixture
def list_schema():
    """Schema for list extraction."""
    return ParsingSchema(
        schema_id="test_list",
        source_id="test",
        start_url="https://test.com",
        item_container="div.product-card",
        fields=[
            FieldDefinition(
                name="name",
                selector="h2.name",
                method=ExtractionMethod.CSS,
            ),
            FieldDefinition(
                name="price",
                selector="span.price",
                method=ExtractionMethod.CSS,
                transformations=["extract_number"],
                type=FieldType.FLOAT,
            ),
        ],
    )


class TestDataExtractorCSS:
    """Tests for CSS selector extraction."""

    def test_extract_text_content(self, simple_html, simple_schema):
        """Test extracting text content with CSS selector."""
        extractor = DataExtractor(simple_schema)
        records = extractor.extract(simple_html)

        assert len(records) == 1
        assert records[0]["title"] == "Test Product"
        assert records[0]["description"] == "Product description here."

    def test_extract_attribute(self, simple_html, simple_schema):
        """Test extracting attribute value."""
        extractor = DataExtractor(simple_schema)
        records = extractor.extract(simple_html)

        assert records[0]["price"] == 29.99

    def test_extract_from_list(self, list_html, list_schema):
        """Test extracting multiple items from container."""
        extractor = DataExtractor(list_schema)
        records = extractor.extract(list_html)

        assert len(records) == 3
        assert records[0]["name"] == "Product One"
        assert records[0]["price"] == 19.99
        assert records[1]["name"] == "Product Two"
        assert records[1]["price"] == 29.99
        assert records[2]["name"] == "Product Three"
        assert records[2]["price"] == 39.99

    def test_extract_with_attribute_in_selector(self, simple_html):
        """Test extracting with @ attribute notation in selector."""
        schema = ParsingSchema(
            schema_id="test_attr",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="link",
                    selector="a.link@href",
                    method=ExtractionMethod.CSS,
                ),
                FieldDefinition(
                    name="image",
                    selector="img@src",
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert records[0]["link"] == "/product/123"
        assert records[0]["image"] == "/images/product.jpg"


class TestDataExtractorXPath:
    """Tests for XPath extraction."""

    def test_xpath_text_extraction(self, simple_html):
        """Test XPath text extraction."""
        schema = ParsingSchema(
            schema_id="test_xpath",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="title",
                    selector="//h1[@class='title']/text()",
                    method=ExtractionMethod.XPATH,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert len(records) == 1
        assert records[0]["title"] == "Test Product"

    def test_xpath_attribute_extraction(self, simple_html):
        """Test XPath attribute extraction."""
        schema = ParsingSchema(
            schema_id="test_xpath_attr",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="price_value",
                    selector="//span[@class='price']/@data-value",
                    method=ExtractionMethod.XPATH,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert records[0]["price_value"] == "29.99"


class TestDataExtractorRegex:
    """Tests for regex extraction."""

    def test_regex_extraction(self, simple_html):
        """Test regex pattern extraction."""
        schema = ParsingSchema(
            schema_id="test_regex",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="product_id",
                    selector=r'/product/(\d+)',
                    method=ExtractionMethod.REGEX,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert records[0]["product_id"] == "123"

    def test_regex_no_group(self, simple_html):
        """Test regex extraction without capture group."""
        schema = ParsingSchema(
            schema_id="test_regex_full",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="price_text",
                    selector=r'\$\d+\.\d+',
                    method=ExtractionMethod.REGEX,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert records[0]["price_text"] == "$29.99"


class TestDataExtractorJSONPath:
    """Tests for JSONPath extraction."""

    def test_jsonpath_extraction(self, json_html):
        """Test JSONPath extraction from embedded JSON."""
        schema = ParsingSchema(
            schema_id="test_jsonpath",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="product_name",
                    selector="$.name",
                    method=ExtractionMethod.JSON_PATH,
                ),
                FieldDefinition(
                    name="price",
                    selector="$.offers.price",
                    method=ExtractionMethod.JSON_PATH,
                    type=FieldType.FLOAT,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(json_html)

        assert len(records) == 1
        assert records[0]["product_name"] == "JSON Product"
        assert records[0]["price"] == 49.99


class TestDataExtractorFallbacks:
    """Tests for fallback selectors."""

    def test_fallback_selector_used(self):
        """Test that fallback selector is used when primary fails."""
        html = """
        <html>
        <body>
            <div class="alternate-price">$99.99</div>
        </body>
        </html>
        """

        schema = ParsingSchema(
            schema_id="test_fallback",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="price",
                    selector=".price",
                    fallback_selectors=[".alt-price", ".alternate-price", ".cost"],
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert records[0]["price"] == "$99.99"

    def test_primary_selector_preferred(self, simple_html):
        """Test that primary selector is used when it works."""
        schema = ParsingSchema(
            schema_id="test_primary",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="title",
                    selector="h1.title",
                    fallback_selectors=["h2.title", ".other-title"],
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(simple_html)

        assert records[0]["title"] == "Test Product"


class TestDataExtractorDefaults:
    """Tests for default values."""

    def test_default_value_used(self):
        """Test that default value is used when extraction fails."""
        html = "<html><body><p>No data here</p></body></html>"

        schema = ParsingSchema(
            schema_id="test_default",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="category",
                    selector=".category",
                    default="Unknown",
                    required=False,
                    method=ExtractionMethod.CSS,
                ),
            ],
            min_fields_required=0,
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert records[0]["category"] == "Unknown"


class TestDataExtractorTransformations:
    """Tests for field transformations."""

    def test_transformations_applied(self, list_html, list_schema):
        """Test that transformations are applied to extracted values."""
        extractor = DataExtractor(list_schema)
        records = extractor.extract(list_html)

        # Prices should be extracted as floats after transformation
        assert isinstance(records[0]["price"], float)
        assert records[0]["price"] == 19.99


class TestDataExtractorTypeConversion:
    """Tests for type conversion."""

    def test_integer_conversion(self):
        """Test integer type conversion."""
        html = "<html><body><span class='count'>42</span></body></html>"

        schema = ParsingSchema(
            schema_id="test_int",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="count",
                    selector=".count",
                    type=FieldType.INTEGER,
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert records[0]["count"] == 42
        assert isinstance(records[0]["count"], int)

    def test_boolean_conversion(self):
        """Test boolean type conversion."""
        html = "<html><body><span class='available'>true</span></body></html>"

        schema = ParsingSchema(
            schema_id="test_bool",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="available",
                    selector=".available",
                    type=FieldType.BOOLEAN,
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert records[0]["available"] is True
        assert isinstance(records[0]["available"], bool)


class TestDataExtractorValidation:
    """Tests for record validation."""

    def test_required_field_missing_invalidates_record(self):
        """Test that missing required fields invalidate the record."""
        html = "<html><body><span class='other'>data</span></body></html>"

        schema = ParsingSchema(
            schema_id="test_required",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="required_field",
                    selector=".required",
                    required=True,
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        assert len(records) == 0

    def test_min_fields_requirement(self):
        """Test minimum fields requirement."""
        html = """
        <html><body>
            <span class='field1'>data1</span>
            <span class='field2'>data2</span>
        </body></html>
        """

        schema = ParsingSchema(
            schema_id="test_min_fields",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(name="field1", selector=".field1", required=False),
                FieldDefinition(name="field2", selector=".field2", required=False),
                FieldDefinition(name="field3", selector=".field3", required=False),
            ],
            min_fields_required=2,
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        # Should pass because 2 fields are found (min is 2)
        assert len(records) == 1

    def test_validation_regex(self):
        """Test field validation with regex."""
        html = """
        <html><body>
            <span class='email'>invalid-email</span>
        </body></html>
        """

        schema = ParsingSchema(
            schema_id="test_validation",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="email",
                    selector=".email",
                    validation_regex=r'^[\w\.-]+@[\w\.-]+\.\w+$',
                    default="default@example.com",
                    required=False,
                    method=ExtractionMethod.CSS,
                ),
            ],
            min_fields_required=0,
        )

        extractor = DataExtractor(schema)
        records = extractor.extract(html)

        # Invalid email should be replaced with default
        assert records[0]["email"] == "default@example.com"


class TestDataExtractorBaseUrl:
    """Tests for base URL handling."""

    def test_base_url_for_transformations(self, simple_html):
        """Test that base URL is passed to transformations."""
        schema = ParsingSchema(
            schema_id="test_base_url",
            source_id="test",
            start_url="https://test.com",
            fields=[
                FieldDefinition(
                    name="link",
                    selector="a.link@href",
                    transformations=["absolute_url"],
                    method=ExtractionMethod.CSS,
                ),
            ],
        )

        extractor = DataExtractor(schema, base_url="https://example.com")
        records = extractor.extract(simple_html)

        assert records[0]["link"] == "https://example.com/product/123"
