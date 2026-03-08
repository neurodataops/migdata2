"""
config_routes.py — Read/write config.yaml (preserving comments via regex)
"""

import re

import yaml
from fastapi import APIRouter, Depends

from api.dependencies import (
    CONFIG_DIR,
    CurrentUser,
    PROJECT_DIR,
    get_config_lock,
)
from api.models import MessageResponse, PlatformUpdate, ThresholdUpdate

# Import from src.config
import sys
sys.path.insert(0, str(PROJECT_DIR / "src"))
from config import clear_config_cache, load_config  # noqa: E402

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("")
def get_config(_user: CurrentUser):
    """Return the full resolved config + executive message."""
    cfg = load_config()

    # Load executive message
    exec_msg = {}
    exec_path = CONFIG_DIR / "executive_message.yaml"
    if exec_path.exists():
        try:
            with open(exec_path, encoding="utf-8") as f:
                exec_msg = yaml.safe_load(f) or {}
        except Exception:
            pass

    return {
        "config": cfg,
        "executive_message": exec_msg,
    }


@router.put("/platform", response_model=MessageResponse)
def update_platform(body: PlatformUpdate, _user: CurrentUser, lock=Depends(get_config_lock)):
    """Update source adapter and/or target platform in config.yaml."""
    config_path = PROJECT_DIR / "config.yaml"

    with lock:
        config_text = config_path.read_text(encoding="utf-8")

        if body.source_adapter is not None:
            config_text = re.sub(
                r'(adapter:\s*)"[^"]*"',
                f'\\1"{body.source_adapter}"',
                config_text,
                count=1,
            )

        if body.target_platform is not None:
            config_text = re.sub(
                r'(platform:\s*)"[^"]*"',
                f'\\1"{body.target_platform}"',
                config_text,
                count=1,
            )

        config_path.write_text(config_text, encoding="utf-8")

    clear_config_cache()
    return MessageResponse(message="Platform config updated.")


@router.put("/threshold", response_model=MessageResponse)
def update_threshold(body: ThresholdUpdate, _user: CurrentUser, lock=Depends(get_config_lock)):
    """Update confidence threshold in config.yaml."""
    config_path = PROJECT_DIR / "config.yaml"

    with lock:
        config_text = config_path.read_text(encoding="utf-8")
        config_text = re.sub(
            r"(confidence_threshold:\s*)[\d.]+",
            f"\\g<1>{body.confidence_threshold}",
            config_text,
            count=1,
        )
        config_path.write_text(config_text, encoding="utf-8")

    clear_config_cache()
    return MessageResponse(message="Threshold updated.")
