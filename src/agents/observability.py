"""
observability.py — Langfuse integration + local trace log for agent tracing
=============================================================================
Wraps Langfuse SDK to trace every LLM call, agent execution,
token usage, latency, and errors.

Also maintains an in-memory trace log so the dashboard Observability tab
can display metrics even when Langfuse keys are not configured.

Set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY env vars to enable Langfuse.
If keys are missing, Langfuse degrades to no-op but local traces still work.
"""

from __future__ import annotations

import os
import time
import uuid
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from src.agents.llm_provider import LLMResponse

logger = logging.getLogger("migdata.observability")

# Maximum number of local trace entries kept in memory
_MAX_LOCAL_TRACES = 500


@dataclass
class TraceEntry:
    """A single trace record stored locally for the observability tab."""
    id: str
    timestamp: str
    agent_name: str
    task_name: str
    provider_id: str
    model: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    latency_ms: float
    success: bool
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "agent_name": self.agent_name,
            "task_name": self.task_name,
            "provider_id": self.provider_id,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "latency_ms": round(self.latency_ms, 1),
            "success": self.success,
            "error": self.error,
        }


@dataclass
class TraceHandle:
    """Opaque handle returned by start_llm_call, passed back to end_llm_call."""
    trace_id: str | None = None
    generation_id: str | None = None
    start_time: float = 0
    agent_name: str = ""
    task_name: str = ""
    provider_id: str = ""
    _langfuse_trace: Any = None
    _langfuse_generation: Any = None


class AgentObservability:
    """
    Langfuse-backed observability for all agent LLM calls.

    Also keeps a local in-memory deque of TraceEntry records so the dashboard
    observability tab can render metrics without external dependencies.

    Initialisation is lazy — the Langfuse client is created on first use
    and only if the required environment variables are set.
    """

    _instance: AgentObservability | None = None
    _langfuse: Any = None
    _enabled: bool = False
    _initialised: bool = False
    _local_traces: deque[TraceEntry]

    def __new__(cls) -> AgentObservability:
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._local_traces = deque(maxlen=_MAX_LOCAL_TRACES)
            cls._instance = inst
        return cls._instance

    def _ensure_init(self) -> None:
        if self._initialised:
            return
        self._initialised = True
        public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
        secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "")
        host = os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com")
        if public_key and secret_key:
            try:
                from langfuse import Langfuse
                self._langfuse = Langfuse(
                    public_key=public_key,
                    secret_key=secret_key,
                    host=host,
                )
                self._enabled = True
                logger.info("Langfuse observability enabled (host=%s)", host)
            except Exception as exc:
                logger.warning("Langfuse init failed, tracing disabled: %s", exc)
                self._enabled = False
        else:
            logger.info("Langfuse keys not set — observability running in no-op mode")
            self._enabled = False

    # ── Public API ────────────────────────────────────────────────────────

    def start_llm_call(
        self,
        agent_name: str,
        provider_id: str,
        system_prompt: str,
        user_prompt: str,
    ) -> TraceHandle:
        """Begin tracing an LLM call. Returns a handle to pass to end_llm_call."""
        self._ensure_init()
        handle = TraceHandle(
            start_time=time.time(),
            agent_name=agent_name,
            provider_id=provider_id,
        )

        if not self._enabled:
            return handle

        try:
            trace = self._langfuse.trace(
                name=f"agent:{agent_name}",
                metadata={"provider": provider_id},
            )
            generation = trace.generation(
                name=f"llm_call:{provider_id}",
                model=provider_id,
                input={"system": system_prompt, "user": user_prompt},
            )
            handle.trace_id = trace.id
            handle.generation_id = generation.id
            handle._langfuse_trace = trace
            handle._langfuse_generation = generation
        except Exception as exc:
            logger.debug("Langfuse start_llm_call failed: %s", exc)

        return handle

    def end_llm_call(
        self,
        trace: TraceHandle,
        response: LLMResponse | None,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Finish tracing an LLM call with the result."""
        latency_ms = (time.time() - trace.start_time) * 1000

        # Always record locally (even without Langfuse)
        input_tokens = response.input_tokens if response else 0
        output_tokens = response.output_tokens if response else 0
        model = response.model if response else trace.provider_id
        self._local_traces.append(TraceEntry(
            id=trace.trace_id or uuid.uuid4().hex[:12],
            timestamp=datetime.now(timezone.utc).isoformat(),
            agent_name=trace.agent_name,
            task_name=trace.task_name,
            provider_id=trace.provider_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
            latency_ms=latency_ms,
            success=success,
            error=error,
        ))

        if not self._enabled or trace._langfuse_generation is None:
            return

        try:
            gen = trace._langfuse_generation
            if success and response is not None:
                gen.end(
                    output=response.content[:500],
                    usage={
                        "input": response.input_tokens,
                        "output": response.output_tokens,
                        "total": response.input_tokens + response.output_tokens,
                    },
                    metadata={
                        "latency_ms": latency_ms,
                        "model": response.model,
                    },
                    level="DEFAULT",
                )
            else:
                gen.end(
                    output=error or "unknown error",
                    level="ERROR",
                    metadata={"latency_ms": latency_ms},
                )
        except Exception as exc:
            logger.debug("Langfuse end_llm_call failed: %s", exc)

    def start_agent_trace(self, agent_name: str, task_name: str, task_input: dict) -> TraceHandle:
        """Begin a top-level agent execution trace."""
        self._ensure_init()
        handle = TraceHandle(start_time=time.time(), agent_name=agent_name, task_name=task_name)

        if not self._enabled:
            return handle

        try:
            trace = self._langfuse.trace(
                name=f"agent:{agent_name}:{task_name}",
                input=task_input,
            )
            handle.trace_id = trace.id
            handle._langfuse_trace = trace
        except Exception as exc:
            logger.debug("Langfuse start_agent_trace failed: %s", exc)

        return handle

    def end_agent_trace(
        self,
        trace: TraceHandle,
        success: bool,
        output: dict | None = None,
        error: str | None = None,
    ) -> None:
        """Finish a top-level agent execution trace."""
        if not self._enabled or trace._langfuse_trace is None:
            return

        try:
            t = trace._langfuse_trace
            t.update(
                output=output or {"error": error},
                metadata={
                    "success": success,
                    "latency_ms": (time.time() - trace.start_time) * 1000,
                },
            )
        except Exception as exc:
            logger.debug("Langfuse end_agent_trace failed: %s", exc)

    # ── Local trace log queries ─────────────────────────────────────────

    def get_traces(self, limit: int = 50) -> list[dict]:
        """Return the most recent trace entries as dicts (newest first)."""
        entries = list(self._local_traces)
        entries.reverse()
        return [e.to_dict() for e in entries[:limit]]

    def get_summary(self) -> dict:
        """Aggregate stats for the observability dashboard."""
        traces = list(self._local_traces)
        if not traces:
            return {
                "total_calls": 0,
                "success_count": 0,
                "error_count": 0,
                "total_tokens": 0,
                "avg_latency_ms": 0,
                "by_provider": {},
                "by_agent": {},
                "langfuse_enabled": self._enabled,
            }
        total = len(traces)
        successes = sum(1 for t in traces if t.success)
        errors = total - successes
        total_tokens = sum(t.total_tokens for t in traces)
        avg_latency = sum(t.latency_ms for t in traces) / total

        by_provider: dict[str, dict] = {}
        for t in traces:
            p = by_provider.setdefault(t.provider_id, {"calls": 0, "tokens": 0, "errors": 0})
            p["calls"] += 1
            p["tokens"] += t.total_tokens
            if not t.success:
                p["errors"] += 1

        by_agent: dict[str, dict] = {}
        for t in traces:
            a = by_agent.setdefault(t.agent_name, {"calls": 0, "tokens": 0, "avg_latency_ms": 0, "errors": 0})
            a["calls"] += 1
            a["tokens"] += t.total_tokens
            if not t.success:
                a["errors"] += 1
        for a in by_agent.values():
            agent_traces = [t for t in traces if t.agent_name == t.agent_name]
            a["avg_latency_ms"] = round(sum(t.latency_ms for t in agent_traces) / len(agent_traces), 1)

        return {
            "total_calls": total,
            "success_count": successes,
            "error_count": errors,
            "total_tokens": total_tokens,
            "avg_latency_ms": round(avg_latency, 1),
            "by_provider": by_provider,
            "by_agent": by_agent,
            "langfuse_enabled": self._enabled,
        }

    def flush(self) -> None:
        """Flush pending Langfuse events (call at shutdown)."""
        if self._enabled and self._langfuse:
            try:
                self._langfuse.flush()
            except Exception:
                pass
