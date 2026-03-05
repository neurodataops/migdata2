"""
config.py — Configuration loader
==================================
Reads config.yaml and resolves paths relative to project root.
Supports environment variable interpolation via ${VAR} syntax.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

_cached_config: dict | None = None


def clear_config_cache() -> None:
    """Reset the cached config so the next load_config() reads from disk."""
    global _cached_config
    _cached_config = None


def _interpolate_env(value: str) -> str:
    """Replace ${VAR} with environment variable values."""
    def replacer(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    return re.sub(r"\$\{(\w+)\}", replacer, value)


def _walk_interpolate(obj: Any) -> Any:
    """Recursively interpolate env vars in all string values."""
    if isinstance(obj, str):
        return _interpolate_env(obj)
    elif isinstance(obj, dict):
        return {k: _walk_interpolate(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_interpolate(v) for v in obj]
    return obj


def load_config(config_path: Path = None) -> dict:
    """Load and cache configuration from YAML file."""
    global _cached_config
    if _cached_config is not None and config_path is None:
        return _cached_config

    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    config = _walk_interpolate(raw)

    if config_path is None:
        _cached_config = config

    return config


def get_path(key: str, config: dict = None) -> Path:
    """
    Resolve a path from config['paths'][key] relative to project root.
    Creates the directory if it doesn't exist.
    """
    cfg = config or load_config()
    rel = cfg.get("paths", {}).get(key, key)
    full = PROJECT_ROOT / rel
    full.mkdir(parents=True, exist_ok=True)
    return full


def get_seed(config: dict = None) -> int:
    cfg = config or load_config()
    return cfg.get("project", {}).get("seed", 42)


def get_source_adapter(config: dict = None) -> str:
    cfg = config or load_config()
    return cfg.get("source", {}).get("adapter", "mock")


def get_conversion_engine(config: dict = None) -> str:
    cfg = config or load_config()
    return cfg.get("conversion", {}).get("engine", "mock")


def get_loader_engine(config: dict = None) -> str:
    cfg = config or load_config()
    return cfg.get("loader", {}).get("engine", "mock")


def get_loader_max_rows(config: dict = None) -> int:
    cfg = config or load_config()
    return cfg.get("loader", {}).get("max_rows", 2000)


def get_validation_engine(config: dict = None) -> str:
    cfg = config or load_config()
    return cfg.get("validation", {}).get("engine", "mock")


def get_validation_weights(config: dict = None) -> dict:
    cfg = config or load_config()
    return cfg.get("validation", {}).get("weights", {
        "row_count": 0.30,
        "checksum": 0.25,
        "null_variance": 0.20,
        "schema_drift": 0.25,
    })


def get_confidence_threshold(config: dict = None) -> float:
    cfg = config or load_config()
    return cfg.get("validation", {}).get("confidence_threshold", 0.6)


def get_source_platform(config: dict = None) -> str:
    """Return the source platform name based on the configured adapter.

    Returns 'snowflake' if the adapter string contains 'snowflake',
    otherwise returns 'redshift'. The legacy value 'mock' maps to 'redshift'.
    """
    adapter = get_source_adapter(config).lower()
    if "snowflake" in adapter:
        return "snowflake"
    return "redshift"


def get_target_platform(config: dict = None) -> str:
    """Return the target platform name from config.
    Defaults to 'databricks' if not specified.
    """
    cfg = config or load_config()
    return cfg.get("target", {}).get("platform", "databricks")


def get_log_level(config: dict = None) -> str:
    cfg = config or load_config()
    return cfg.get("logging", {}).get("level", "INFO")


def get_json_logs(config: dict = None) -> bool:
    cfg = config or load_config()
    return cfg.get("logging", {}).get("json_logs", True)
