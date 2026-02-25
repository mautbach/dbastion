"""The `auth` command group: manage database credentials."""

from __future__ import annotations

import click

from dbastion.auth import (
    bigquery_oauth_flow,
    load_credentials,
    remove_credentials,
    store_credentials,
)


@click.group()
def auth() -> None:
    """Manage database credentials."""


@auth.command()
@click.option(
    "--client-secrets",
    type=click.Path(exists=True),
    default=None,
    help="Path to custom OAuth client_secrets.json. Uses built-in client ID if omitted.",
)
def bigquery(client_secrets: str | None) -> None:
    """Authenticate with BigQuery via browser OAuth flow."""
    click.echo("Opening browser for Google sign-in...")

    try:
        creds_data = bigquery_oauth_flow(client_secrets)
    except ImportError as e:
        click.echo(f"error: {e}", err=True)
        raise SystemExit(1) from e
    except Exception as e:
        click.echo(f"error: OAuth flow failed: {e}", err=True)
        raise SystemExit(1) from e

    path = store_credentials("bigquery", creds_data)
    click.echo(f"Credentials saved to {path}")
    click.echo("You can now run: dbastion query \"SELECT 1\" --db bigquery:project=YOUR_PROJECT")


@auth.command()
@click.argument("provider", type=click.Choice(["bigquery", "postgres"]))
def status(provider: str) -> None:
    """Check if credentials are stored for a provider."""
    creds = load_credentials(provider)
    if creds is not None:
        click.echo(f"{provider}: authenticated")
    else:
        click.echo(f"{provider}: no stored credentials (will fall back to ADC)")


@auth.command()
@click.argument("provider", type=click.Choice(["bigquery", "postgres"]))
def logout(provider: str) -> None:
    """Remove stored credentials for a provider."""
    if remove_credentials(provider):
        click.echo(f"{provider}: credentials removed")
    else:
        click.echo(f"{provider}: no stored credentials to remove")
