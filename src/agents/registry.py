"""
registry.py — Agent Registry
==============================
Central registry for discovering, listing, and invoking agents.
Agents register themselves at import time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.agents.base_agent import BaseAgent


class AgentRegistry:
    """Singleton registry of all available agents."""

    _instance: AgentRegistry | None = None
    _agents: dict[str, BaseAgent] = {}

    def __new__(cls) -> AgentRegistry:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._agents = {}
        return cls._instance

    def register(self, agent: BaseAgent) -> None:
        self._agents[agent.agent_name] = agent

    def get(self, name: str) -> BaseAgent | None:
        return self._agents.get(name)

    def list_agents(self) -> list[dict]:
        return [
            {
                "agent_name": a.agent_name,
                "agent_description": a.agent_description,
            }
            for a in self._agents.values()
        ]

    def ensure_defaults(self) -> None:
        """Import and register all built-in agents."""
        if "sql_transpilation" not in self._agents:
            from src.agents.sql_transpilation_agent import SQLTranspilationAgent
            self.register(SQLTranspilationAgent())
