"""
Unit tests for access_control module.

Tests user_can_access, get_access_context, and related helpers.
Uses unittest.mock to avoid real storage/GCS and to simulate auth enabled/disabled.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

# Import after patching env so we can control _auth_enabled behavior
import access_control as ac

OWNER_KEY = ac.OWNER_KEY
EDITORS_KEY = ac.EDITORS_KEY
RESTRICT_KEY = ac.RESTRICT_KEY
DEFAULT_DOMAIN = ac.DEFAULT_DOMAIN


# ---------------------------------------------------------------------------
# Pure function tests (no mocks)
# ---------------------------------------------------------------------------


class TestNormalizeEmail:
    def test_empty_or_none(self):
        assert ac.normalize_email("") == ""
        assert ac.normalize_email(None) == ""
        assert ac.normalize_email("   ") == ""

    def test_lowercase_and_strip(self):
        assert ac.normalize_email("  Alice@BasePowerCompany.com  ") == "alice@basepowercompany.com"

    def test_preserves_valid_email(self):
        assert ac.normalize_email("user@basepowercompany.com") == "user@basepowercompany.com"


class TestIsCompanyEmail:
    def test_valid_company_email(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            assert ac.is_company_email("alice@basepowercompany.com") is True
            assert ac.is_company_email("bob@basepowercompany.com") is True

    def test_wrong_domain(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            assert ac.is_company_email("alice@gmail.com") is False
            assert ac.is_company_email("alice@other.com") is False

    def test_empty_or_invalid(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            assert ac.is_company_email("") is False
            assert ac.is_company_email("no-at-sign") is False


class TestNormalizeEmailList:
    def test_from_list(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            raw = ["alice@basepowercompany.com", "  Bob@BasePowerCompany.com  ", "alice@basepowercompany.com"]
            result = ac.normalize_email_list(raw)
            assert result == ["alice@basepowercompany.com", "bob@basepowercompany.com"]

    def test_from_string_with_newlines(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            raw = "alice@basepowercompany.com\nbob@basepowercompany.com"
            result = ac.normalize_email_list(raw)
            assert result == ["alice@basepowercompany.com", "bob@basepowercompany.com"]

    def test_filters_non_company_emails(self):
        with patch.dict(os.environ, {"ALLOWED_DOMAIN": DEFAULT_DOMAIN}, clear=False):
            raw = ["alice@basepowercompany.com", "bob@gmail.com", "charlie@basepowercompany.com"]
            result = ac.normalize_email_list(raw)
            assert result == ["alice@basepowercompany.com", "charlie@basepowercompany.com"]


class TestAllowedUsersSet:
    def test_owner_and_editors(self):
        settings = {
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com", "bob@basepowercompany.com"],
        }
        allowed = ac._allowed_users_set(settings)
        assert allowed == {
            "owner@basepowercompany.com",
            "alice@basepowercompany.com",
            "bob@basepowercompany.com",
        }

    def test_owner_implicitly_included(self):
        settings = {
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: [],
        }
        allowed = ac._allowed_users_set(settings)
        assert "owner@basepowercompany.com" in allowed

    def test_empty_settings(self):
        allowed = ac._allowed_users_set({})
        assert allowed == set()


# ---------------------------------------------------------------------------
# user_can_access (requires auth mock)
# ---------------------------------------------------------------------------


class TestUserCanAccess:
    """Test user_can_access with auth enabled (GOOGLE_CLIENT_ID set)."""

    @pytest.fixture(autouse=True)
    def auth_enabled(self):
        with patch.dict(
            os.environ,
            {"GOOGLE_CLIENT_ID": "test-id", "GOOGLE_CLIENT_SECRET": "test-secret"},
            clear=False,
        ):
            yield

    def test_no_restrict_any_company_user_allowed(self, auth_enabled):
        settings = {OWNER_KEY: "owner@basepowercompany.com", EDITORS_KEY: []}
        assert ac.user_can_access("alice@basepowercompany.com", settings) is True
        assert ac.user_can_access("bob@basepowercompany.com", settings) is True

    def test_restrict_on_owner_allowed(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        assert ac.user_can_access("owner@basepowercompany.com", settings) is True

    def test_restrict_on_editor_allowed(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        assert ac.user_can_access("alice@basepowercompany.com", settings) is True

    def test_restrict_on_other_user_denied(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        assert ac.user_can_access("bob@basepowercompany.com", settings) is False

    def test_restrict_on_empty_user_denied(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: [],
        }
        assert ac.user_can_access("", settings) is False

    def test_restrict_on_non_company_email_denied(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: [],
        }
        assert ac.user_can_access("bob@gmail.com", settings) is False


class TestUserCanAccessAuthDisabled:
    """When auth is disabled, everyone is allowed regardless of restrict."""

    @pytest.fixture(autouse=True)
    def auth_disabled(self):
        with patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "", "GOOGLE_CLIENT_SECRET": ""}, clear=False):
            yield

    def test_restrict_ignored_when_auth_disabled(self, auth_disabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: [],
        }
        assert ac.user_can_access("bob@basepowercompany.com", settings) is True


# ---------------------------------------------------------------------------
# get_access_context
# ---------------------------------------------------------------------------


class TestGetAccessContext:
    @pytest.fixture(autouse=True)
    def auth_enabled(self):
        with patch.dict(
            os.environ,
            {"GOOGLE_CLIENT_ID": "test-id", "GOOGLE_CLIENT_SECRET": "test-secret"},
            clear=False,
        ):
            yield

    def test_owner_role(self, auth_enabled):
        settings = {
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        ctx = ac.get_access_context(settings, user_email="owner@basepowercompany.com")
        assert ctx["role"] == "owner"
        assert ctx["can_edit_settings"] is True
        assert ctx["can_manage_access"] is True
        assert ctx["access_denied"] is False

    def test_editor_role(self, auth_enabled):
        settings = {
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        ctx = ac.get_access_context(settings, user_email="alice@basepowercompany.com")
        assert ctx["role"] == "editor"
        assert ctx["can_edit_settings"] is True
        assert ctx["can_manage_access"] is False
        assert ctx["access_denied"] is False

    def test_viewer_role(self, auth_enabled):
        settings = {
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        ctx = ac.get_access_context(settings, user_email="bob@basepowercompany.com")
        assert ctx["role"] == "viewer"
        assert ctx["can_edit_settings"] is False
        assert ctx["can_manage_access"] is False
        assert ctx["access_denied"] is False

    def test_access_denied_when_restricted(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        ctx = ac.get_access_context(settings, user_email="bob@basepowercompany.com")
        assert ctx["access_denied"] is True
        assert ctx["role"] == "viewer"

    def test_access_allowed_when_restricted_but_user_in_list(self, auth_enabled):
        settings = {
            RESTRICT_KEY: True,
            OWNER_KEY: "owner@basepowercompany.com",
            EDITORS_KEY: ["alice@basepowercompany.com"],
        }
        ctx = ac.get_access_context(settings, user_email="alice@basepowercompany.com")
        assert ctx["access_denied"] is False
        assert ctx["role"] == "editor"

    def test_restrict_flag_in_context(self, auth_enabled):
        settings = {RESTRICT_KEY: True, OWNER_KEY: "o@basepowercompany.com", EDITORS_KEY: []}
        ctx = ac.get_access_context(settings, user_email="o@basepowercompany.com")
        assert ctx["restrict_access_to_editors_only"] is True
