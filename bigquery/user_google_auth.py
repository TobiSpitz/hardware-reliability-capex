"""
Helpers for using the signed-in dashboard user's Google credentials.

This module is request-context aware:
- In Cloud Run request handlers, it can build user credentials from Flask session.
- Outside request handlers (jobs, scripts), it returns None and callers should
  use default service-account credentials.
"""
from __future__ import annotations

import os
from typing import Iterable

from flask import has_request_context, session
from google.oauth2.credentials import Credentials

from auth import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_TOKEN_URL,
    get_google_access_token,
)


DEFAULT_CLOUD_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/cloud-platform",
)


def user_credential_mode_enabled() -> bool:
    """Return whether user-credential mode is enabled by env flag."""
    raw = str(os.environ.get("USE_SIGNED_IN_USER_GCP", "false") or "").strip().lower()
    return raw in {"1", "true", "yes", "y"}


def _normalize_scopes(scopes: Iterable[str] | None) -> list[str]:
    if scopes is None:
        return list(DEFAULT_CLOUD_SCOPES)
    out = [str(s).strip() for s in scopes if str(s).strip()]
    return out or list(DEFAULT_CLOUD_SCOPES)


def get_signed_in_user_credentials(scopes: Iterable[str] | None = None) -> Credentials | None:
    """Build google.oauth2.credentials.Credentials from Flask session.

    Returns None when:
    - called outside request context
    - user mode is disabled
    - no active OAuth token in session
    """
    if not user_credential_mode_enabled():
        return None
    if not has_request_context():
        return None

    token = get_google_access_token()
    if not token:
        return None

    requested_scopes = _normalize_scopes(scopes)
    refresh_token = str(session.get("google_refresh_token", "") or "").strip()
    client_id = str(GOOGLE_CLIENT_ID or "").strip()
    client_secret = str(GOOGLE_CLIENT_SECRET or "").strip()

    if refresh_token and client_id and client_secret:
        return Credentials(
            token=token,
            refresh_token=refresh_token,
            token_uri=GOOGLE_TOKEN_URL,
            client_id=client_id,
            client_secret=client_secret,
            scopes=requested_scopes,
        )
    return Credentials(token=token, scopes=requested_scopes)

