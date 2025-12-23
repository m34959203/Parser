"""Prompts for AI-powered schema generation."""

STRUCTURE_ANALYSIS_PROMPT = """You are an expert web scraping analyst. Analyze the webpage structure to identify extractable data.

<examples>
<example>
<input>
URL: https://shop.example.com/products
Goal: Extract product catalog
DOM: div.products-grid > div.product-card (×24)
</input>
<output>
{
  "page_type": "product_catalog",
  "repeating_container": "div.products-grid",
  "repeating_item": "div.product-card",
  "fields": [
    {"name": "title", "selector": "h3.product-name", "type": "string", "confidence": 0.95},
    {"name": "price", "selector": "span.price", "type": "float", "confidence": 0.92},
    {"name": "image", "selector": "img.product-img", "attribute": "src", "type": "url", "confidence": 0.98}
  ],
  "pagination": {"type": "next_button", "selector": "a.pagination-next"},
  "requires_js": false
}
</output>
</example>

<example>
<input>
URL: https://news.example.com/article/12345
Goal: Extract article content
DOM: article.main-content > ...
</input>
<output>
{
  "page_type": "article",
  "repeating_container": null,
  "repeating_item": null,
  "fields": [
    {"name": "headline", "selector": "h1.article-title", "type": "string", "confidence": 0.98},
    {"name": "author", "selector": "span.author-name", "type": "string", "confidence": 0.85},
    {"name": "published_date", "selector": "time.pub-date", "attribute": "datetime", "type": "datetime", "confidence": 0.90},
    {"name": "content", "selector": "div.article-body", "type": "string", "confidence": 0.95}
  ],
  "pagination": null,
  "requires_js": false
}
</output>
</example>
</examples>

Now analyze this page:

URL: {url}
Goal: {goal}
DOM Structure:
{dom_tree}

HTML snippet (first 5000 chars):
{html_snippet}

Identify:
1. What type of page is this? (catalog, product page, article, listing, etc.)
2. What repeating elements exist? (product cards, list items, etc.)
3. What data fields can be extracted?
4. Is there pagination? What type?
5. Does the page require JavaScript rendering?

Respond ONLY with valid JSON matching the format in the examples above."""


SCHEMA_GENERATION_PROMPT = """Generate a complete parsing schema based on the page analysis.

Page Analysis:
{analysis}

User Goal: {goal}
Requested Fields: {example_fields}
Constraints: {constraints}

Generate a complete ParsingSchema in JSON with:
1. Precise CSS selectors (prefer classes over complex paths)
2. Appropriate data types and transformations
3. Validation rules where applicable
4. Pagination configuration if detected
5. Fallback selectors for important fields

Required schema format:
{{
    "schema_id": "auto_{source_id}",
    "version": "1.0.0",
    "source_id": "{source_id}",
    "description": "...",
    "start_url": "{url}",
    "item_container": "...",  // CSS selector for repeating container, null if single page
    "fields": [
        {{
            "name": "field_name",
            "type": "string|integer|float|boolean|datetime|url|list",
            "method": "css",
            "selector": "...",
            "attribute": null,  // or "href", "src", etc.
            "required": true,
            "default": null,
            "transformations": ["trim", "extract_number", etc.],
            "fallback_selectors": ["...", "..."]
        }}
    ],
    "pagination": {{
        "type": "next_button|page_param|infinite_scroll|none",
        "selector": "...",
        "max_pages": 50
    }},
    "dedup_keys": ["field1", "field2"],
    "mode": "http|browser",
    "requires_js": false
}}

IMPORTANT:
- Use simple, robust selectors that are unlikely to break
- Add fallback selectors for critical fields
- Include appropriate data transformations
- Set requires_js=true if JavaScript rendering is needed

Respond ONLY with valid JSON."""


SELECTOR_IMPROVEMENT_PROMPT = """The selector "{selector}" failed to extract data.

Error: {error}

HTML context around the expected element:
{html_context}

Current field configuration:
{field_config}

Suggest 3 alternative selectors from most to least specific:
1. Most specific (using classes and attributes)
2. Medium specificity (using parent-child relationships)
3. Most robust (using text content or data attributes)

Respond in JSON:
{{
    "alternatives": [
        {{"selector": "...", "method": "css", "confidence": 0.0-1.0, "explanation": "..."}},
        {{"selector": "...", "method": "css", "confidence": 0.0-1.0, "explanation": "..."}},
        {{"selector": "...", "method": "xpath", "confidence": 0.0-1.0, "explanation": "..."}}
    ],
    "recommended_index": 0,
    "notes": "..."
}}"""
