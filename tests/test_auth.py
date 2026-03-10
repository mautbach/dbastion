"""Test credential management."""

import os
from unittest.mock import patch

import pytest

from dbastion.auth import (
    load_bigquery_credentials,
    load_credentials,
    remove_credentials,
    store_credentials,
)


def test_store_and_load(tmp_path):
    creds_dir = tmp_path / "credentials"
    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        store_credentials("bigquery", {"refresh_token": "test-token"})
        loaded = load_credentials("bigquery")

    assert loaded == {"refresh_token": "test-token"}


def test_store_creates_file_mode_600(tmp_path):
    creds_dir = tmp_path / "credentials"
    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        path = store_credentials("bigquery", {"token": "secret"})

    mode = os.stat(path).st_mode & 0o777
    assert mode == 0o600


def test_load_missing_returns_none(tmp_path):
    creds_dir = tmp_path / "credentials"
    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        assert load_credentials("bigquery") is None


def test_remove_existing(tmp_path):
    creds_dir = tmp_path / "credentials"
    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        store_credentials("bigquery", {"token": "x"})
        assert remove_credentials("bigquery") is True
        assert load_credentials("bigquery") is None


def test_remove_nonexistent(tmp_path):
    creds_dir = tmp_path / "credentials"
    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        assert remove_credentials("bigquery") is False


def test_auth_status_cli(tmp_path):
    """Test auth status command."""
    from click.testing import CliRunner

    from dbastion.cli.auth import auth

    creds_dir = tmp_path / "credentials"
    runner = CliRunner()

    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        result = runner.invoke(auth, ["status", "bigquery"])
        assert "no stored credentials" in result.output

        store_credentials("bigquery", {"token": "x"})
        result = runner.invoke(auth, ["status", "bigquery"])
        assert "authenticated" in result.output


def test_auth_logout_cli(tmp_path):
    """Test auth logout command."""
    from click.testing import CliRunner

    from dbastion.cli.auth import auth

    creds_dir = tmp_path / "credentials"
    runner = CliRunner()

    with patch("dbastion.auth._CREDENTIALS_DIR", creds_dir):
        store_credentials("bigquery", {"token": "x"})
        result = runner.invoke(auth, ["logout", "bigquery"])
        assert "credentials removed" in result.output
        assert load_credentials("bigquery") is None


# -- load_bigquery_credentials fallback tests --


def _has_google_auth() -> bool:
    try:
        import google.auth  # noqa: F401
        import google.oauth2.credentials  # noqa: F401

        return True
    except ImportError:
        return False


_skip_no_google_auth = pytest.mark.skipif(
    not _has_google_auth(),
    reason="google-auth / google-cloud-bigquery not installed",
)


@_skip_no_google_auth
def test_load_bq_creds_no_stored_no_adc_returns_none(tmp_path):
    """No stored creds and ADC unavailable → (None, 'none')."""
    creds_dir = tmp_path / "credentials"
    with (
        patch("dbastion.auth._CREDENTIALS_DIR", creds_dir),
        patch("google.auth.default", side_effect=Exception("no ADC")),
    ):
        creds, source = load_bigquery_credentials()
    assert creds is None
    assert source == "none"


@_skip_no_google_auth
def test_load_bq_creds_invalid_stored_falls_back_to_adc(tmp_path):
    """Invalid stored creds should warn and fall back to ADC."""
    creds_dir = tmp_path / "credentials"
    sentinel = object()
    with (
        patch("dbastion.auth._CREDENTIALS_DIR", creds_dir),
        patch("google.auth.default", return_value=(sentinel, "project-id")),
    ):
        store_credentials("bigquery", {"bad": "data"})
        creds, source = load_bigquery_credentials()
    assert creds is sentinel
    assert source == "adc"


@_skip_no_google_auth
def test_load_bq_creds_valid_stored_returns_stored(tmp_path):
    """Valid stored creds should return (creds, 'stored')."""
    creds_dir = tmp_path / "credentials"
    sentinel = object()
    with (
        patch("dbastion.auth._CREDENTIALS_DIR", creds_dir),
        patch(
            "google.oauth2.credentials.Credentials.from_authorized_user_info",
            return_value=sentinel,
        ),
    ):
        creds_data = {
            "refresh_token": "good",
            "client_id": "x",
            "client_secret": "y",
        }
        store_credentials("bigquery", creds_data)
        creds, source = load_bigquery_credentials()
    assert creds is sentinel
    assert source == "stored"
