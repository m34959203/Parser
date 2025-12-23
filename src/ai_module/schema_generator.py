"""AI-powered schema generation using LLMs."""

import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from anthropic import Anthropic
from playwright.async_api import async_playwright
import structlog

from src.config import AISettings, get_settings
from src.shared.models import FieldDefinition, PaginationRule, ParsingSchema
from .prompts import STRUCTURE_ANALYSIS_PROMPT, SCHEMA_GENERATION_PROMPT
from .validator import SchemaValidator, TestResult

logger = structlog.get_logger()


@dataclass
class PageData:
    """Crawled page data."""
    url: str
    html: str
    screenshot: bytes | None = None
    dom_tree: dict | None = None


@dataclass
class StructureField:
    """Detected field from structure analysis."""
    name: str
    selector: str
    type: str
    confidence: float
    attribute: str | None = None


@dataclass
class StructureAnalysis:
    """Result of page structure analysis."""
    page_type: str
    repeating_container: str | None
    repeating_item: str | None
    fields: list[StructureField]
    pagination: dict | None
    requires_js: bool
    notes: list[str] = field(default_factory=list)


@dataclass
class GenerationRequest:
    """Request for schema generation."""
    url: str
    goal_description: str
    example_fields: list[str] | None = None
    constraints: dict | None = None


@dataclass
class GenerationResult:
    """Result of schema generation."""
    schema: ParsingSchema
    confidence: float
    warnings: list[str]
    test_results: list[TestResult]


class SchemaGenerator:
    """AI-powered schema generator using Claude."""

    def __init__(self, config: AISettings | None = None):
        self.config = config or get_settings().ai
        self._client = self._create_client()
        self._validator = SchemaValidator()

    def _create_client(self):
        """Create LLM client based on configuration."""
        if self.config.provider == "anthropic":
            return Anthropic(
                api_key=self.config.anthropic_api_key.get_secret_value()
                if self.config.anthropic_api_key
                else None
            )
        elif self.config.provider == "openai":
            from openai import OpenAI
            return OpenAI(
                api_key=self.config.openai_api_key.get_secret_value()
                if self.config.openai_api_key
                else None
            )
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        """Generate a parsing schema for a URL.

        Args:
            request: Generation request with URL and goal

        Returns:
            Generated schema with confidence and test results
        """
        logger.info("Starting schema generation", url=request.url, goal=request.goal_description)

        # Step 1: Crawl the page
        page_data = await self._crawl_page(request.url)

        # Step 2: Analyze structure
        structure = await self._analyze_structure(page_data, request.goal_description)

        # Step 3: Generate schema
        schema = await self._generate_schema(structure, request)

        # Step 4: Validate schema
        test_results = await self._validator.validate_schema(schema, request.url)

        # Step 5: Fix if needed
        if not test_results.success:
            schema = await self.improve_schema(
                schema=schema,
                test_url=request.url,
                issues=[e for e in test_results.errors],
            )
            test_results = await self._validator.validate_schema(schema, request.url)

        # Calculate confidence
        confidence = self._calculate_confidence(structure, test_results)

        # Collect warnings
        warnings = self._collect_warnings(structure, test_results)

        logger.info(
            "Schema generation complete",
            schema_id=schema.schema_id,
            confidence=confidence,
            warnings_count=len(warnings),
        )

        return GenerationResult(
            schema=schema,
            confidence=confidence,
            warnings=warnings,
            test_results=[test_results],
        )

    async def _crawl_page(self, url: str) -> PageData:
        """Fetch page content using Playwright."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            )
            page = await context.new_page()

            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)

                html = await page.content()
                screenshot = await page.screenshot(full_page=True)

                # Extract DOM structure
                dom_tree = await page.evaluate("""
                    () => {
                        function getTree(el, depth = 0) {
                            if (depth > 4 || !el) return null;
                            const children = Array.from(el.children || [])
                                .slice(0, 10)
                                .map(c => getTree(c, depth + 1))
                                .filter(Boolean);
                            return {
                                tag: el.tagName?.toLowerCase(),
                                classes: Array.from(el.classList || []).slice(0, 5),
                                id: el.id || null,
                                text: el.textContent?.slice(0, 50)?.trim() || null,
                                childCount: el.children?.length || 0,
                                children: children
                            };
                        }
                        return getTree(document.body);
                    }
                """)

                return PageData(
                    url=url,
                    html=html,
                    screenshot=screenshot,
                    dom_tree=dom_tree,
                )

            finally:
                await browser.close()

    async def _analyze_structure(
        self,
        page_data: PageData,
        goal: str,
    ) -> StructureAnalysis:
        """Analyze page structure using LLM."""
        prompt = STRUCTURE_ANALYSIS_PROMPT.format(
            url=page_data.url,
            goal=goal,
            dom_tree=json.dumps(page_data.dom_tree, indent=2)[:8000],
            html_snippet=page_data.html[:5000],
        )

        response = self._call_llm(prompt)
        data = self._extract_json(response)

        fields = [
            StructureField(
                name=f["name"],
                selector=f["selector"],
                type=f.get("type", "string"),
                confidence=f.get("confidence", 0.8),
                attribute=f.get("attribute"),
            )
            for f in data.get("fields", [])
        ]

        return StructureAnalysis(
            page_type=data.get("page_type", "unknown"),
            repeating_container=data.get("repeating_container"),
            repeating_item=data.get("repeating_item"),
            fields=fields,
            pagination=data.get("pagination"),
            requires_js=data.get("requires_js", False),
            notes=data.get("notes", []),
        )

    async def _generate_schema(
        self,
        structure: StructureAnalysis,
        request: GenerationRequest,
    ) -> ParsingSchema:
        """Generate full parsing schema from structure analysis."""
        parsed_url = urlparse(request.url)
        source_id = parsed_url.netloc + parsed_url.path

        prompt = SCHEMA_GENERATION_PROMPT.format(
            analysis=json.dumps({
                "page_type": structure.page_type,
                "repeating_container": structure.repeating_container,
                "repeating_item": structure.repeating_item,
                "fields": [
                    {
                        "name": f.name,
                        "selector": f.selector,
                        "type": f.type,
                        "confidence": f.confidence,
                        "attribute": f.attribute,
                    }
                    for f in structure.fields
                ],
                "pagination": structure.pagination,
                "requires_js": structure.requires_js,
            }, indent=2),
            goal=request.goal_description,
            example_fields=request.example_fields or "auto-detect",
            constraints=json.dumps(request.constraints) if request.constraints else "none",
            source_id=source_id.replace("/", "_").replace(".", "_"),
            url=request.url,
        )

        response = self._call_llm(prompt)
        schema_data = self._extract_json(response)

        # Build ParsingSchema
        return ParsingSchema(
            schema_id=schema_data.get("schema_id", f"auto_{parsed_url.netloc}"),
            version=schema_data.get("version", "1.0.0"),
            source_id=schema_data.get("source_id", source_id),
            description=schema_data.get("description", request.goal_description),
            start_url=request.url,
            item_container=schema_data.get("item_container") or structure.repeating_item,
            fields=[
                FieldDefinition(**f)
                for f in schema_data.get("fields", [])
            ],
            pagination=PaginationRule(**schema_data["pagination"])
            if schema_data.get("pagination")
            else None,
            dedup_keys=schema_data.get("dedup_keys", []),
            mode="browser" if structure.requires_js else "http",
            requires_js=structure.requires_js,
            confidence=self._calculate_field_confidence(structure.fields),
        )

    async def improve_schema(
        self,
        schema: ParsingSchema,
        test_url: str,
        issues: list[str],
    ) -> ParsingSchema:
        """Improve a failing schema based on issues."""
        from .prompts import SELECTOR_IMPROVEMENT_PROMPT

        # Crawl page again for context
        page_data = await self._crawl_page(test_url)

        improved_fields = []

        for field_def in schema.fields:
            field_issues = [i for i in issues if field_def.name in i]

            if field_issues:
                # Get improvement suggestions
                prompt = SELECTOR_IMPROVEMENT_PROMPT.format(
                    selector=field_def.selector,
                    error="; ".join(field_issues),
                    html_context=page_data.html[:3000],
                    field_config=field_def.model_dump_json(indent=2),
                )

                response = self._call_llm(prompt)
                suggestions = self._extract_json(response)

                if suggestions.get("alternatives"):
                    best = suggestions["alternatives"][suggestions.get("recommended_index", 0)]
                    improved_field = field_def.model_copy(
                        update={
                            "selector": best["selector"],
                            "fallback_selectors": [
                                alt["selector"]
                                for alt in suggestions["alternatives"][1:]
                            ],
                        }
                    )
                    improved_fields.append(improved_field)
                else:
                    improved_fields.append(field_def)
            else:
                improved_fields.append(field_def)

        # Create improved schema
        return schema.model_copy(
            update={
                "fields": improved_fields,
                "version": self._increment_version(schema.version),
            }
        )

    def _call_llm(self, prompt: str) -> str:
        """Call the configured LLM."""
        if self.config.provider == "anthropic":
            response = self._client.messages.create(
                model=self.config.model_name,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        elif self.config.provider == "openai":
            response = self._client.chat.completions.create(
                model=self.config.model_name,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.choices[0].message.content
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")

    def _extract_json(self, text: str) -> dict:
        """Extract JSON from LLM response."""
        # Try to find JSON block
        json_match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))

        # Try to find raw JSON
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))

        raise ValueError(f"No valid JSON found in response: {text[:200]}")

    def _calculate_confidence(
        self,
        structure: StructureAnalysis,
        test_result,
    ) -> float:
        """Calculate overall confidence score."""
        # Base confidence from structure analysis
        field_confidence = self._calculate_field_confidence(structure.fields)

        # Adjust based on test results
        if test_result.success:
            return min(field_confidence + 0.1, 1.0)
        else:
            # Reduce based on failed fields
            failed_ratio = len(test_result.errors) / max(len(structure.fields), 1)
            return field_confidence * (1 - failed_ratio * 0.5)

    def _calculate_field_confidence(self, fields: list[StructureField]) -> float:
        """Calculate average field confidence."""
        if not fields:
            return 0.5
        return sum(f.confidence for f in fields) / len(fields)

    def _collect_warnings(
        self,
        structure: StructureAnalysis,
        test_result,
    ) -> list[str]:
        """Collect warnings from analysis and testing."""
        warnings = []

        if structure.requires_js:
            warnings.append("Page requires JavaScript rendering - use browser mode")

        low_confidence_fields = [
            f.name for f in structure.fields if f.confidence < 0.8
        ]
        if low_confidence_fields:
            warnings.append(f"Low confidence for fields: {', '.join(low_confidence_fields)}")

        if test_result.errors:
            warnings.append(f"Extraction issues: {'; '.join(test_result.errors[:3])}")

        return warnings

    def _increment_version(self, version: str) -> str:
        """Increment patch version."""
        parts = version.split(".")
        parts[-1] = str(int(parts[-1]) + 1)
        return ".".join(parts)
