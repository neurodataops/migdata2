"""
models.py — Pydantic request/response models
"""

from pydantic import BaseModel, Field


# ── Auth ────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class RegisterRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=6)


class AuthResponse(BaseModel):
    token: str
    username: str


class MessageResponse(BaseModel):
    message: str


# ── Config ──────────────────────────────────────────────────────────────────

class PlatformUpdate(BaseModel):
    source_adapter: str | None = None
    target_platform: str | None = None


class ThresholdUpdate(BaseModel):
    confidence_threshold: float = Field(ge=0.0, le=1.0)


# ── Connection ──────────────────────────────────────────────────────────────

class RedshiftConnectionRequest(BaseModel):
    host: str
    port: int = 5439
    database: str
    user: str
    password: str


class SnowflakeConnectionRequest(BaseModel):
    account: str
    warehouse: str
    database: str
    role: str
    user: str
    password: str


class DatabricksConnectionRequest(BaseModel):
    host: str
    http_path: str
    access_token: str


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    hint: str = ""


# ── Pipeline ────────────────────────────────────────────────────────────────

class PipelineRunRequest(BaseModel):
    source_platform: str = "snowflake"
    use_mock: bool = True
    selected_schemas: list[str] = []  # Empty means all schemas


class PipelineStatusResponse(BaseModel):
    running: bool
    current_step: int = 0
    total_steps: int = 0
    label: str = ""
    job_id: str = ""
