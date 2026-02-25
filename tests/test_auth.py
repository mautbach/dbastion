"""Test credential management."""

import os
from unittest.mock import patch

from dbastion.auth import load_credentials, remove_credentials, store_credentials


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
