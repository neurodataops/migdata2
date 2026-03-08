"""
dependencies.py — FastAPI dependencies (JWT auth, project paths, file lock)
"""

import os
from pathlib import Path
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from filelock import FileLock
from jose import JWTError, jwt

PROJECT_DIR = Path(__file__).resolve().parent.parent
MOCK_DATA_DIR = PROJECT_DIR / "mock_data"
ARTIFACTS_DIR = PROJECT_DIR / "artifacts"
TRANSPILED_DIR = ARTIFACTS_DIR / "transpiled_sql"
TARGET_DIR = ARTIFACTS_DIR / "target_tables"
TEST_RESULTS_DIR = PROJECT_DIR / "test_results"
CONFIG_DIR = PROJECT_DIR / "config"
LOGS_DIR = ARTIFACTS_DIR / "logs"

JWT_SECRET = os.environ.get("MIGDATA_JWT_SECRET", "migdata-dev-secret-change-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24

_config_lock = FileLock(str(PROJECT_DIR / "config.yaml.lock"), timeout=10)

security = HTTPBearer()


def get_config_lock() -> FileLock:
    return _config_lock


def verify_token(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> str:
    """Validate JWT token and return username."""
    try:
        payload = jwt.decode(
            credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM]
        )
        username: str | None = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing subject",
            )
        return username
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


CurrentUser = Annotated[str, Depends(verify_token)]
