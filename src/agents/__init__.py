"""
agents — AI-First Agent Framework for MigData
===============================================
Provides a pluggable multi-agent architecture with:
- Multi-LLM provider abstraction (Claude, GPT, DeepSeek, Llama)
- Langfuse observability for all agent calls
- Agent registry for discovery and invocation
"""

from src.agents.base_agent import BaseAgent
from src.agents.llm_provider import LLMProvider, LLMProviderRegistry
from src.agents.observability import AgentObservability
from src.agents.registry import AgentRegistry

__all__ = [
    "BaseAgent",
    "LLMProvider",
    "LLMProviderRegistry",
    "AgentObservability",
    "AgentRegistry",
]
