"""Bearer token auth (optional, driven by Settings.api_token)."""

from __future__ import annotations

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from arkham_api.settings import Settings

bearer = HTTPBearer(auto_error=False)


def require_auth(
    settings: Settings, credentials: HTTPAuthorizationCredentials | None
) -> None:
    """Require a bearer token when settings.api_token is set.

    If API_TOKEN is unset, auth is disabled (local dev convenience).
    """
    if not settings.api_token:
        return
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Missing bearer token")
    if credentials.credentials != settings.api_token:
        raise HTTPException(status_code=403, detail="Invalid token")
