"""
Dashboard settings access control helpers.

Roles:
- owner: can manage access list and edit major settings/operations
- editor: can edit major settings/operations (not access list)
- viewer: read-only for major settings
"""
from __future__ import annotations

import os
from typing import Any

from flask import session

import storage_backend as store

OWNER_KEY = "settings_owner_email"
EDITORS_KEY = "settings_editor_emails"
RESTRICT_KEY = "restrict_access_to_editors_only"
DEFAULT_DOMAIN = "basepowercompany.com"


def _auth_enabled() -> bool:
    return bool(os.environ.get("GOOGLE_CLIENT_ID", "") and os.environ.get("GOOGLE_CLIENT_SECRET", ""))


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def company_domain() -> str:
    raw = str(os.environ.get("ALLOWED_DOMAIN", DEFAULT_DOMAIN) or DEFAULT_DOMAIN).strip().lower()
    return raw if raw else DEFAULT_DOMAIN


def is_company_email(value: str) -> bool:
    email = normalize_email(value)
    return bool(email and "@" in email and email.endswith("@" + company_domain()))


def normalize_email_list(raw: Any) -> list[str]:
    if isinstance(raw, str):
        candidates = [p for p in raw.replace(",", "\n").splitlines()]
    elif isinstance(raw, list):
        candidates = [str(v) for v in raw]
    else:
        candidates = []
    deduped: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        email = normalize_email(c)
        if not email or not is_company_email(email) or email in seen:
            continue
        seen.add(email)
        deduped.append(email)
    return deduped


def current_user_email() -> str:
    return normalize_email(session.get("user_email", ""))


def ensure_access_defaults(settings: dict[str, Any], *, bootstrap_user_email: str = "") -> tuple[dict[str, Any], bool]:
    changed = False
    out = dict(settings or {})
    owner = normalize_email(out.get(OWNER_KEY))
    env_owner = normalize_email(os.environ.get("SETTINGS_OWNER_EMAIL", ""))
    if not owner and is_company_email(env_owner):
        owner = env_owner
        changed = True
    if not owner and is_company_email(bootstrap_user_email):
        owner = normalize_email(bootstrap_user_email)
        changed = True

    editors = normalize_email_list(out.get(EDITORS_KEY, []))
    if owner and owner not in editors:
        editors.insert(0, owner)
        changed = True

    if out.get(OWNER_KEY) != owner:
        out[OWNER_KEY] = owner
        changed = True
    if out.get(EDITORS_KEY) != editors:
        out[EDITORS_KEY] = editors
        changed = True

    return out, changed


def load_settings_with_access_defaults(*, bootstrap_user_email: str = "") -> tuple[dict[str, Any], bool]:
    raw = store.read_json("dashboard_settings.json")
    settings = raw if isinstance(raw, dict) else {}
    return ensure_access_defaults(settings, bootstrap_user_email=bootstrap_user_email)


def _allowed_users_set(settings: dict[str, Any]) -> set[str]:
    """Return the set of emails allowed to access when restrict_access_to_editors_only is True."""
    owner = normalize_email(settings.get(OWNER_KEY))
    editors = normalize_email_list(settings.get(EDITORS_KEY, []))
    allowed = set(editors)
    if owner:
        allowed.add(owner)
    return allowed


def user_can_access(user_email: str, settings: dict[str, Any] | None = None) -> bool:
    """
    Return True if the user is allowed to access the dashboard.
    When restrict_access_to_editors_only is True, only owner and editors can access.
    When False (default), any @company-domain user can access (viewer or higher).
    Recovery: SETTINGS_OWNER_EMAIL env var always allows that user (for lockout recovery).
    """
    if not _auth_enabled():
        return True
    if settings is None:
        settings, _ = load_settings_with_access_defaults(bootstrap_user_email=user_email)
    user = normalize_email(user_email)
    if not user or not is_company_email(user):
        return False
    # Recovery: env owner is always allowed (prevents lockout)
    env_owner = normalize_email(os.environ.get("SETTINGS_OWNER_EMAIL", ""))
    if env_owner and user == env_owner:
        return True
    restrict = bool(settings.get(RESTRICT_KEY, False))
    if not restrict:
        return True
    return user in _allowed_users_set(settings)


def get_access_context(settings: dict[str, Any] | None = None, *, user_email: str = "") -> dict[str, Any]:
    user = normalize_email(user_email) or current_user_email()

    if settings is None:
        settings, _ = load_settings_with_access_defaults(bootstrap_user_email=user)

    owner = normalize_email(settings.get(OWNER_KEY))
    editors = normalize_email_list(settings.get(EDITORS_KEY, []))
    editors_set = set(editors)
    if owner:
        editors_set.add(owner)

    if not _auth_enabled():
        role = "owner"
        can_edit_settings = True
        can_manage_access = True
    elif owner and user == owner:
        role = "owner"
        can_edit_settings = True
        can_manage_access = True
    elif user and user in editors_set:
        role = "editor"
        can_edit_settings = True
        can_manage_access = False
    else:
        role = "viewer"
        can_edit_settings = False
        can_manage_access = False

    restrict = bool(settings.get(RESTRICT_KEY, False))
    access_denied = (
        _auth_enabled()
        and restrict
        and user
        and user not in _allowed_users_set(settings)
    )

    return {
        "auth_enabled": _auth_enabled(),
        "role": role,
        "user_email": user,
        "owner_email": owner,
        "editor_emails": sorted(editors_set),
        "can_edit_settings": can_edit_settings,
        "can_manage_access": can_manage_access,
        "restrict_access_to_editors_only": restrict,
        "access_denied": access_denied,
    }
