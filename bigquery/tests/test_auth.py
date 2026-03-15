"""
Unit tests for auth module.

Tests _url_quote, access-denied page rendering, and auth flow logic.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from flask import Flask

# Import auth to test helpers; init_auth is tested via integration
import auth as auth_module


class TestUrlQuote:
    def test_quotes_spaces(self):
        assert "%20" in auth_module._url_quote("hello world")

    def test_quotes_newlines(self):
        result = auth_module._url_quote("line1\nline2")
        assert "%0A" in result or "\\n" in result or "\n" not in result

    def test_empty_string(self):
        assert auth_module._url_quote("") == ""

    def test_special_chars_in_mailto_body(self):
        body = "Hi,\n\nI would like to request access.\n\nMy email: user@example.com"
        result = auth_module._url_quote(body)
        assert " " not in result or "%20" in result
        assert "@" in result or "%40" in result


class TestAuthAccessDeniedRoute:
    """Test /auth/access-denied route renders correctly."""

    @pytest.fixture
    def app(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        with patch.object(auth_module, "_auth_enabled", return_value=True):
            auth_module.init_auth(app)
        return app

    @pytest.fixture
    def client(self, app):
        return app.test_client()

    def test_access_denied_requires_login(self, client):
        """Without session, before_request redirects to login-page."""
        rv = client.get("/")
        assert rv.status_code == 302
        assert "login" in rv.location.lower() or "auth" in rv.location.lower()

    def test_access_denied_page_with_session(self, client):
        """With session but no access, /auth/access-denied renders."""
        with client.session_transaction() as sess:
            sess["user_email"] = "blocked@basepowercompany.com"

        with patch("access_control.load_settings_with_access_defaults") as mock_load:
            mock_load.return_value = (
                {
                    "settings_owner_email": "owner@basepowercompany.com",
                    "settings_editor_emails": [],
                    "restrict_access_to_editors_only": True,
                },
                False,
            )
            rv = client.get("/auth/access-denied")
        assert rv.status_code == 200
        assert b"Request access" in rv.data or b"request access" in rv.data.lower()
        assert b"blocked@basepowercompany.com" in rv.data
        assert b"Email owner" in rv.data or b"email owner" in rv.data.lower()
        assert b"mailto:" in rv.data

    def test_access_denied_mailto_has_owner(self, client):
        """Request access link includes owner email when owner is set."""
        with client.session_transaction() as sess:
            sess["user_email"] = "blocked@basepowercompany.com"

        with patch("access_control.load_settings_with_access_defaults") as mock_load:
            mock_load.return_value = (
                {
                    "settings_owner_email": "owner@basepowercompany.com",
                    "settings_editor_emails": [],
                },
                False,
            )
            with patch("auth.get_access_context") as mock_ctx:
                mock_ctx.return_value = {"owner_email": "owner@basepowercompany.com"}
                rv = client.get("/auth/access-denied")
        assert rv.status_code == 200
        assert b"mailto:owner@basepowercompany.com" in rv.data
