"""
auth.py — Login & register endpoints
"""

import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, status
from jose import jwt

from api.dependencies import (
    CONFIG_DIR,
    JWT_ALGORITHM,
    JWT_EXPIRE_HOURS,
    JWT_SECRET,
)
from api.models import AuthResponse, LoginRequest, MessageResponse, RegisterRequest

router = APIRouter(prefix="/api/auth", tags=["auth"])

USERS_FILE = CONFIG_DIR / "users.json"


def _load_users() -> list[dict]:
    if USERS_FILE.exists():
        data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        return data.get("users", [])
    return []


def _save_users(users: list[dict]):
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps({"users": users}, indent=2), encoding="utf-8")


def _create_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    return jwt.encode(
        {"sub": username, "exp": expire}, JWT_SECRET, algorithm=JWT_ALGORITHM
    )


@router.post("/login", response_model=AuthResponse)
def login(body: LoginRequest):
    for u in _load_users():
        if u["username"] == body.username and u["password"] == body.password:
            return AuthResponse(
                token=_create_token(body.username), username=body.username
            )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password.",
    )


@router.post("/register", response_model=MessageResponse)
def register(body: RegisterRequest):
    users = _load_users()
    if any(u["username"] == body.username for u in users):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already exists.",
        )
    users.append(
        {
            "username": body.username,
            "password": body.password,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    _save_users(users)
    return MessageResponse(message="Registration successful! You can now log in.")
