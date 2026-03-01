"""CLI entry point. Both `dbastion` and `dbast` resolve here."""

from __future__ import annotations

import click

from dbastion.cli.auth import auth
from dbastion.cli.connect import connect
from dbastion.cli.exec import exec_cmd
from dbastion.cli.query import query
from dbastion.cli.schema import schema
from dbastion.cli.validate import validate


@click.group()
@click.version_option(package_name="dbastion")
def main() -> None:
    """dbastion: governed database access for AI agents."""


main.add_command(auth)
main.add_command(connect)
main.add_command(schema)
main.add_command(validate)
main.add_command(query)
main.add_command(exec_cmd)
