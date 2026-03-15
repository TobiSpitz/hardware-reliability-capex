"""
Provider-agnostic LLM interface for classification review.

Adapters for Anthropic (Claude), OpenAI (GPT), and Google Vertex AI.
Each adapter handles its own auth and returns structured ClassificationResult objects.
"""
from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ClassificationResult:
    """Structured output from an LLM classification review."""
    item_index: int
    station_id: str | None
    station_confidence: float
    mfg_subcategory: str
    subcat_confidence: float
    reasoning: str
    agrees_with_rules: bool


class LLMAdapter(ABC):
    """Base interface for LLM classification review."""

    @abstractmethod
    def classify_batch(
        self,
        system_prompt: str,
        items: list[dict],
    ) -> list[ClassificationResult]:
        """Send a batch of items for classification review.

        Args:
            system_prompt: Full system prompt with business context and examples.
            items: List of item dicts (vendor, description, price, rule results, etc.)

        Returns:
            One ClassificationResult per item.
        """
        ...

    def _build_user_prompt(self, items: list[dict]) -> str:
        """Format items into the user message."""
        lines = ["Review the following items and classify each one.\n"]
        lines.append("Respond with a JSON array. Each element must have:")
        lines.append('  item_index, station_id (or null), station_confidence (0-1),')
        lines.append('  mfg_subcategory, subcat_confidence (0-1), reasoning, agrees_with_rules (bool)')
        lines.append("\nItems:\n")
        for i, item in enumerate(items):
            lines.append(f"[{i}] {json.dumps(item)}")
        return "\n".join(lines)

    def _parse_response(self, text: str, expected_count: int) -> list[ClassificationResult]:
        """Parse JSON array from LLM response text."""
        text = text.strip()
        start = text.find("[")
        end = text.rfind("]") + 1
        if start == -1 or end == 0:
            return self._empty_results(expected_count)

        try:
            parsed = json.loads(text[start:end])
        except json.JSONDecodeError:
            return self._empty_results(expected_count)

        results: list[ClassificationResult] = []
        for i, item in enumerate(parsed):
            if not isinstance(item, dict):
                continue
            results.append(ClassificationResult(
                item_index=item.get("item_index", i),
                station_id=item.get("station_id"),
                station_confidence=float(item.get("station_confidence", 0)),
                mfg_subcategory=item.get("mfg_subcategory", ""),
                subcat_confidence=float(item.get("subcat_confidence", 0)),
                reasoning=item.get("reasoning", ""),
                agrees_with_rules=bool(item.get("agrees_with_rules", True)),
            ))
        return results

    def _empty_results(self, count: int) -> list[ClassificationResult]:
        return [
            ClassificationResult(
                item_index=i, station_id=None, station_confidence=0,
                mfg_subcategory="", subcat_confidence=0,
                reasoning="LLM response parse failed", agrees_with_rules=True,
            )
            for i in range(count)
        ]


class AnthropicAdapter(LLMAdapter):
    """Claude via Anthropic's direct API."""

    def __init__(
        self,
        *,
        api_key: str = "",
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 4096,
    ):
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._model = model
        self._max_tokens = max_tokens

    def classify_batch(
        self,
        system_prompt: str,
        items: list[dict],
    ) -> list[ClassificationResult]:
        import anthropic

        client = anthropic.Anthropic(api_key=self._api_key)
        user_msg = self._build_user_prompt(items)

        response = client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = response.content[0].text
        return self._parse_response(text, len(items))


class OpenAIAdapter(LLMAdapter):
    """GPT-4o / GPT-4o-mini via OpenAI API."""

    def __init__(
        self,
        *,
        api_key: str = "",
        model: str = "gpt-4o",
        max_tokens: int = 4096,
    ):
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._model = model
        self._max_tokens = max_tokens

    def classify_batch(
        self,
        system_prompt: str,
        items: list[dict],
    ) -> list[ClassificationResult]:
        import openai

        client = openai.OpenAI(api_key=self._api_key)
        user_msg = self._build_user_prompt(items)

        response = client.chat.completions.create(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
        )
        text = response.choices[0].message.content or ""
        return self._parse_response(text, len(items))


class VertexAdapter(LLMAdapter):
    """Claude via Google Vertex AI (uses GCP ADC, no separate API key needed)."""

    def __init__(
        self,
        *,
        project: str = "",
        region: str = "us-east5",
        model: str = "claude-sonnet-4@20250514",
        max_tokens: int = 4096,
    ):
        self._project = project or os.environ.get("BQ_ANALYTICS_PROJECT", "mfg-eng-19197")
        self._region = region
        self._model = model
        self._max_tokens = max_tokens

    def classify_batch(
        self,
        system_prompt: str,
        items: list[dict],
    ) -> list[ClassificationResult]:
        from anthropic import AnthropicVertex

        client = AnthropicVertex(
            project_id=self._project,
            region=self._region,
        )
        user_msg = self._build_user_prompt(items)

        response = client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = response.content[0].text
        return self._parse_response(text, len(items))


class GeminiAdapter(LLMAdapter):
    """Gemini via Vertex AI -- uses GCP Application Default Credentials.

    No API key needed -- authenticates using the same GCP ADC as BigQuery.
    """

    def __init__(
        self,
        *,
        project: str = "",
        location: str = "us-central1",
        model: str = "gemini-2.5-pro",
        max_tokens: int = 8192,
    ):
        self._project = project or os.environ.get("BQ_ANALYTICS_PROJECT", "mfg-eng-19197")
        self._location = location
        self._model = model
        self._max_tokens = max_tokens

    def classify_batch(
        self,
        system_prompt: str,
        items: list[dict],
    ) -> list[ClassificationResult]:
        from google import genai
        from google.genai.types import GenerateContentConfig
        from user_google_auth import get_signed_in_user_credentials

        user_creds = get_signed_in_user_credentials()
        client_kwargs: dict[str, Any] = {
            "vertexai": True,
            "project": self._project,
            "location": self._location,
        }
        if user_creds is not None:
            client_kwargs["credentials"] = user_creds
        client = genai.Client(**client_kwargs)
        user_msg = self._build_user_prompt(items)

        response = client.models.generate_content(
            model=self._model,
            contents=user_msg,
            config=GenerateContentConfig(
                system_instruction=system_prompt,
                max_output_tokens=self._max_tokens,
                temperature=0.1,
                response_mime_type="application/json",
            ),
        )
        text = response.text
        return self._parse_response(text, len(items))


def get_adapter(provider: str = "") -> LLMAdapter:
    """Factory: return the appropriate adapter based on provider name or env config."""
    provider = provider or os.environ.get("LLM_PROVIDER", "gemini").lower()

    if provider == "gemini":
        return GeminiAdapter()
    elif provider == "anthropic":
        return AnthropicAdapter()
    elif provider == "openai":
        return OpenAIAdapter()
    elif provider == "vertex":
        return VertexAdapter()
    else:
        raise ValueError(
            f"Unknown LLM provider: {provider}. "
            "Use: gemini, anthropic, openai, vertex"
        )
