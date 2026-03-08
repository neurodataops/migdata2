"""
llm_provider.py — Multi-LLM Provider Abstraction
==================================================
Unified interface for calling different LLM providers.
Supports: Claude Opus 4, GPT-4o Mini, DeepSeek-V3, Llama 3.3 70B.
"""

from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMResponse:
    """Standardised response from any LLM provider."""
    content: str
    model: str
    provider: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0
    raw: dict = field(default_factory=dict)


@dataclass
class LLMProviderInfo:
    """Metadata about an available LLM provider."""
    id: str
    name: str
    model_id: str
    provider: str
    description: str


# ── Available providers exposed to the UI ────────────────────────────────────

AVAILABLE_PROVIDERS: list[LLMProviderInfo] = [
    LLMProviderInfo(
        id="claude-opus-4",
        name="Claude Opus 4",
        model_id="claude-opus-4-20250514",
        provider="anthropic",
        description="Anthropic's most capable model — best for complex SQL reasoning",
    ),
    LLMProviderInfo(
        id="gpt-4o-mini",
        name="GPT-4o Mini",
        model_id="gpt-4o-mini",
        provider="openai",
        description="OpenAI's fast and cost-effective model",
    ),
    LLMProviderInfo(
        id="deepseek-v3",
        name="DeepSeek-V3",
        model_id="deepseek-chat",
        provider="deepseek",
        description="DeepSeek's flagship open-weight reasoning model",
    ),
    LLMProviderInfo(
        id="llama-3.3-70b",
        name="Llama 3.3 70B",
        model_id="meta-llama/Llama-3.3-70B-Instruct-Turbo",
        provider="together",
        description="Meta's open-source Llama via Together AI",
    ),
]


class LLMProvider(ABC):
    """Abstract base for LLM provider implementations."""

    @abstractmethod
    def chat(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        ...

    @abstractmethod
    def provider_id(self) -> str:
        ...


# ── Concrete providers ───────────────────────────────────────────────────────

class AnthropicProvider(LLMProvider):
    """Claude via the Anthropic SDK."""

    def __init__(self, model: str = "claude-opus-4-20250514", api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    def provider_id(self) -> str:
        return "claude-opus-4"

    def chat(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)
        t0 = time.time()
        resp = client.messages.create(
            model=self.model,
            max_tokens=kwargs.get("max_tokens", 4096),
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        latency = (time.time() - t0) * 1000
        content = resp.content[0].text if resp.content else ""
        return LLMResponse(
            content=content,
            model=self.model,
            provider="anthropic",
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            latency_ms=latency,
            raw={"id": resp.id, "stop_reason": resp.stop_reason},
        )


class OpenAIProvider(LLMProvider):
    """GPT via the OpenAI SDK."""

    def __init__(self, model: str = "gpt-4o-mini", api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "")

    def provider_id(self) -> str:
        return "gpt-4o-mini"

    def chat(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        import openai
        client = openai.OpenAI(api_key=self.api_key)
        t0 = time.time()
        resp = client.chat.completions.create(
            model=self.model,
            max_tokens=kwargs.get("max_tokens", 4096),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        latency = (time.time() - t0) * 1000
        choice = resp.choices[0]
        usage = resp.usage
        return LLMResponse(
            content=choice.message.content or "",
            model=self.model,
            provider="openai",
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=latency,
            raw={"id": resp.id, "finish_reason": choice.finish_reason},
        )


class DeepSeekProvider(LLMProvider):
    """DeepSeek via OpenAI-compatible API."""

    def __init__(self, model: str = "deepseek-chat", api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY", "")

    def provider_id(self) -> str:
        return "deepseek-v3"

    def chat(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        import openai
        client = openai.OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com",
        )
        t0 = time.time()
        resp = client.chat.completions.create(
            model=self.model,
            max_tokens=kwargs.get("max_tokens", 4096),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        latency = (time.time() - t0) * 1000
        choice = resp.choices[0]
        usage = resp.usage
        return LLMResponse(
            content=choice.message.content or "",
            model=self.model,
            provider="deepseek",
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=latency,
            raw={"id": resp.id, "finish_reason": choice.finish_reason},
        )


class TogetherProvider(LLMProvider):
    """Llama via Together AI (OpenAI-compatible API)."""

    def __init__(self, model: str = "meta-llama/Llama-3.3-70B-Instruct-Turbo",
                 api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.environ.get("TOGETHER_API_KEY", "")

    def provider_id(self) -> str:
        return "llama-3.3-70b"

    def chat(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        import openai
        client = openai.OpenAI(
            api_key=self.api_key,
            base_url="https://api.together.xyz/v1",
        )
        t0 = time.time()
        resp = client.chat.completions.create(
            model=self.model,
            max_tokens=kwargs.get("max_tokens", 4096),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        latency = (time.time() - t0) * 1000
        choice = resp.choices[0]
        usage = resp.usage
        return LLMResponse(
            content=choice.message.content or "",
            model=self.model,
            provider="together",
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=latency,
            raw={"id": resp.id, "finish_reason": choice.finish_reason},
        )


# ── Registry ─────────────────────────────────────────────────────────────────

class LLMProviderRegistry:
    """Singleton registry for LLM providers.  Keeps track of the active provider."""

    _instance: LLMProviderRegistry | None = None
    _active_provider_id: str = "claude-opus-4"  # default
    _providers: dict[str, LLMProvider] = {}

    def __new__(cls) -> LLMProviderRegistry:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._providers = {}
        return cls._instance

    # ── public API ────────────────────────────────────────────────────────

    def register(self, provider: LLMProvider) -> None:
        self._providers[provider.provider_id()] = provider

    def set_active(self, provider_id: str) -> None:
        if provider_id not in [p.id for p in AVAILABLE_PROVIDERS]:
            raise ValueError(f"Unknown provider: {provider_id}")
        self._active_provider_id = provider_id

    def get_active(self) -> LLMProvider:
        """Return the currently active provider, lazily instantiating if needed."""
        if self._active_provider_id not in self._providers:
            self._providers[self._active_provider_id] = _build_provider(self._active_provider_id)
        return self._providers[self._active_provider_id]

    @property
    def active_provider_id(self) -> str:
        return self._active_provider_id

    @staticmethod
    def list_providers() -> list[dict]:
        return [
            {
                "id": p.id,
                "name": p.name,
                "model_id": p.model_id,
                "provider": p.provider,
                "description": p.description,
            }
            for p in AVAILABLE_PROVIDERS
        ]


def _build_provider(provider_id: str) -> LLMProvider:
    """Lazily build a concrete provider from its id."""
    mapping = {
        "claude-opus-4": AnthropicProvider,
        "gpt-4o-mini": OpenAIProvider,
        "deepseek-v3": DeepSeekProvider,
        "llama-3.3-70b": TogetherProvider,
    }
    cls = mapping.get(provider_id)
    if cls is None:
        raise ValueError(f"No implementation for provider: {provider_id}")
    return cls()
