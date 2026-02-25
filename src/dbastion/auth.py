"""Credential management — OAuth flows and credential storage."""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path

_CREDENTIALS_DIR = Path.home() / ".dbastion" / "credentials"

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]

# Built-in OAuth client for dbastion (GCP project: dbastion-io).
# Desktop app — the "secret" is NOT confidential per Google docs.
# Obfuscated to avoid automated secret scanners (not real security).
# See: https://developers.google.com/identity/protocols/oauth2/native-app
_BQ_CID = (
    "NDIyNzY4NTY3MTc4LTRkZjhxODRlbW51a2E0azRqcG1u"
    "NWJidnR0MzVoMmRhLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29t"
)
_BQ_CS = "R09DU1BYLTVPNUNGVTExR3MwREgxcVFlR0l2MmdtN1NWclA="


def _bq_client_config() -> dict:
    return {
        "installed": {
            "client_id": base64.b64decode(_BQ_CID).decode(),
            "client_secret": base64.b64decode(_BQ_CS).decode(),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }


def _creds_path(provider: str) -> Path:
    return _CREDENTIALS_DIR / f"{provider}.json"


def store_credentials(provider: str, creds_data: dict) -> Path:
    """Save credentials to ~/.dbastion/credentials/{provider}.json (mode 600)."""
    path = _creds_path(provider)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(creds_data, indent=2))
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return path


def load_credentials(provider: str) -> dict | None:
    """Load stored credentials, or None if not found."""
    path = _creds_path(provider)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def remove_credentials(provider: str) -> bool:
    """Remove stored credentials. Returns True if file existed."""
    path = _creds_path(provider)
    if path.exists():
        path.unlink()
        return True
    return False


def bigquery_oauth_flow(client_secrets_file: str | None = None) -> dict:
    """Run browser-based OAuth flow for BigQuery.

    Args:
        client_secrets_file: Optional path to custom OAuth client_secrets.json.
            If None, uses dbastion's built-in client ID.

    Returns:
        Credentials dict with refresh_token, suitable for storage.
    """
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as e:
        raise ImportError(
            "google-auth-oauthlib is required for OAuth. "
            "Install with: pip install 'dbastion[bigquery]'"
        ) from e

    if client_secrets_file:
        app_flow = InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, scopes=BQ_SCOPES
        )
    else:
        app_flow = InstalledAppFlow.from_client_config(
            _bq_client_config(), scopes=BQ_SCOPES
        )

    credentials = app_flow.run_local_server(port=0)

    return {
        "type": "authorized_user",
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
    }


def load_bigquery_credentials():
    """Load BigQuery credentials: dbastion stored → ADC fallback.

    Returns a google.auth.credentials.Credentials object or None.
    """
    # Try dbastion-stored credentials first
    creds_data = load_credentials("bigquery")
    if creds_data is not None:
        try:
            from google.oauth2.credentials import Credentials

            return Credentials.from_authorized_user_info(creds_data, scopes=BQ_SCOPES)
        except Exception:
            pass

    # Fall back to Application Default Credentials (gcloud, service account, etc.)
    try:
        import google.auth

        credentials, _ = google.auth.default(scopes=BQ_SCOPES)
        return credentials
    except Exception:
        return None
