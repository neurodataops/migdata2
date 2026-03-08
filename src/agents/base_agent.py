"""
base_agent.py — Abstract base class for all MigData agents
============================================================
Every agent must subclass BaseAgent and implement `execute()`.
The base class wires up LLM access and Langfuse observability automatically.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from src.agents.llm_provider import LLMProviderRegistry, LLMResponse
from src.agents.observability import AgentObservability


@dataclass
class AgentResult:
    """Standardised result returned by every agent task."""
    success: bool
    data: dict = field(default_factory=dict)
    error: str | None = None
    llm_calls: int = 0
    total_tokens: int = 0
    latency_ms: float = 0


class BaseAgent(ABC):
    """
    Abstract base for every MigData agent.

    Subclasses define:
        - agent_name: str          (unique identifier)
        - agent_description: str   (short human-readable purpose)
        - execute(task_input)      (main entry point)
    """

    agent_name: str = "base_agent"
    agent_description: str = ""

    def __init__(self) -> None:
        self._llm_registry = LLMProviderRegistry()
        self._observability = AgentObservability()

    # ── LLM helpers ──────────────────────────────────────────────────────

    def call_llm(self, system_prompt: str, user_prompt: str, **kwargs: Any) -> LLMResponse:
        """Call the currently active LLM with observability tracing."""
        provider = self._llm_registry.get_active()
        trace = self._observability.start_llm_call(
            agent_name=self.agent_name,
            provider_id=provider.provider_id(),
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        try:
            response = provider.chat(system_prompt, user_prompt, **kwargs)
            self._observability.end_llm_call(
                trace=trace,
                response=response,
                success=True,
            )
            return response
        except Exception as exc:
            self._observability.end_llm_call(
                trace=trace,
                response=None,
                success=False,
                error=str(exc),
            )
            raise

    def parse_json_response(self, content: str) -> dict:
        """Extract JSON from an LLM response that may contain markdown fences."""
        text = content.strip()
        # Strip markdown code fences
        if text.startswith("```"):
            first_nl = text.index("\n")
            last_fence = text.rfind("```")
            text = text[first_nl + 1:last_fence].strip()
        return json.loads(text)

    # ── Abstract contract ────────────────────────────────────────────────

    @abstractmethod
    def execute(self, task_input: dict) -> AgentResult:
        """
        Run the agent's task.

        Parameters
        ----------
        task_input : dict
            Task-specific payload (varies per agent).

        Returns
        -------
        AgentResult with success flag, data payload, and metrics.
        """
        ...

    # ── Convenience ──────────────────────────────────────────────────────

    def info(self) -> dict:
        return {
            "agent_name": self.agent_name,
            "agent_description": self.agent_description,
            "active_llm": self._llm_registry.active_provider_id,
        }
