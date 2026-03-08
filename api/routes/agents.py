"""
agents.py — API routes for the agent framework
=================================================
Endpoints for LLM provider management and agent execution.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.agents.llm_provider import LLMProviderRegistry
from src.agents.observability import AgentObservability
from src.agents.registry import AgentRegistry

router = APIRouter(prefix="/api/agents", tags=["agents"])


# ── Request / Response models ────────────────────────────────────────────────

class SetProviderRequest(BaseModel):
    provider_id: str


class TranspileDDLRequest(BaseModel):
    source_ddl: str
    source_dialect: str = ""  # "Redshift", "Snowflake", or "" for auto-detect
    task_id: int = 1


class TranspileQueryRequest(BaseModel):
    source_sql: str
    source_dialect: str = ""  # "Redshift", "Snowflake", or "" for auto-detect


# ── LLM Provider endpoints ──────────────────────────────────────────────────

@router.get("/llm-providers")
def list_llm_providers():
    """List all available LLM providers and which is active."""
    registry = LLMProviderRegistry()
    return {
        "providers": registry.list_providers(),
        "active": registry.active_provider_id,
    }


@router.put("/llm-provider")
def set_llm_provider(req: SetProviderRequest):
    """Set the active LLM provider."""
    registry = LLMProviderRegistry()
    try:
        registry.set_active(req.provider_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"active": registry.active_provider_id}


# ── Agent listing ────────────────────────────────────────────────────────────

@router.get("/")
def list_agents():
    """List all registered agents."""
    reg = AgentRegistry()
    reg.ensure_defaults()
    return {"agents": reg.list_agents()}


# ── SQL Transpilation Agent endpoints ────────────────────────────────────────

@router.post("/sql-transpilation/transpile-ddl")
def transpile_ddl(req: TranspileDDLRequest):
    """
    Execute a SQL Transpilation Agent task.
    Task 1: Convert a CREATE TABLE DDL (Redshift/Snowflake/mock) to Databricks.
    """
    reg = AgentRegistry()
    reg.ensure_defaults()
    agent = reg.get("sql_transpilation")
    if agent is None:
        raise HTTPException(status_code=500, detail="SQL Transpilation Agent not registered")

    result = agent.execute({
        "task_id": req.task_id,
        "source_ddl": req.source_ddl,
        "source_dialect": req.source_dialect,
    })

    return {
        "success": result.success,
        "data": result.data,
        "error": result.error,
        "metrics": {
            "llm_calls": result.llm_calls,
            "total_tokens": result.total_tokens,
            "latency_ms": round(result.latency_ms, 1),
        },
    }


@router.post("/sql-transpilation/transpile-query")
def transpile_query(req: TranspileQueryRequest):
    """
    Execute SQL Transpilation Agent Task 2.
    Convert SELECT queries with JOINs preserving aliases and join order.
    """
    reg = AgentRegistry()
    reg.ensure_defaults()
    agent = reg.get("sql_transpilation")
    if agent is None:
        raise HTTPException(status_code=500, detail="SQL Transpilation Agent not registered")

    result = agent.execute({
        "task_id": 2,
        "source_sql": req.source_sql,
        "source_dialect": req.source_dialect,
    })

    return {
        "success": result.success,
        "data": result.data,
        "error": result.error,
        "metrics": {
            "llm_calls": result.llm_calls,
            "total_tokens": result.total_tokens,
            "latency_ms": round(result.latency_ms, 1),
        },
    }


@router.post("/sql-transpilation/transpile-cte")
def transpile_cte(req: TranspileQueryRequest):
    """
    Execute SQL Transpilation Agent Task 3.
    Convert CTE queries (WITH ... AS) with nested sub-CTEs to Databricks SQL.
    """
    reg = AgentRegistry()
    reg.ensure_defaults()
    agent = reg.get("sql_transpilation")
    if agent is None:
        raise HTTPException(status_code=500, detail="SQL Transpilation Agent not registered")

    result = agent.execute({
        "task_id": 3,
        "source_sql": req.source_sql,
        "source_dialect": req.source_dialect,
    })

    return {
        "success": result.success,
        "data": result.data,
        "error": result.error,
        "metrics": {
            "llm_calls": result.llm_calls,
            "total_tokens": result.total_tokens,
            "latency_ms": round(result.latency_ms, 1),
        },
    }


@router.post("/sql-transpilation/transpile-window")
def transpile_window(req: TranspileQueryRequest):
    """
    Execute SQL Transpilation Agent Task 4.
    Translate window functions (ROW_NUMBER, RANK, SUM OVER, etc.) to Databricks SQL.
    """
    reg = AgentRegistry()
    reg.ensure_defaults()
    agent = reg.get("sql_transpilation")
    if agent is None:
        raise HTTPException(status_code=500, detail="SQL Transpilation Agent not registered")

    result = agent.execute({
        "task_id": 4,
        "source_sql": req.source_sql,
        "source_dialect": req.source_dialect,
    })

    return {
        "success": result.success,
        "data": result.data,
        "error": result.error,
        "metrics": {
            "llm_calls": result.llm_calls,
            "total_tokens": result.total_tokens,
            "latency_ms": round(result.latency_ms, 1),
        },
    }


# ── Observability endpoints ──────────────────────────────────────────────────

@router.get("/observability/traces")
def get_traces(limit: int = 50):
    """Return recent agent trace entries for the observability tab."""
    obs = AgentObservability()
    return {"traces": obs.get_traces(limit=limit)}


@router.get("/observability/summary")
def get_observability_summary():
    """Return aggregate observability stats."""
    obs = AgentObservability()
    return obs.get_summary()
