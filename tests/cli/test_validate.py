"""Test the validate CLI command end-to-end."""

import json

from click.testing import CliRunner

from dbastion.cli import main


def test_validate_safe_select() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "SELECT id FROM users"])
    assert result.exit_code == 0


def test_validate_delete_without_where_blocked() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "DELETE FROM users", "--allow-write"])
    assert result.exit_code == 1
    assert "DELETE without WHERE" in result.output


def test_validate_multiple_statements_blocked() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "SELECT 1; DROP TABLE x"])
    assert result.exit_code == 1
    assert "multiple statements" in result.output


def test_validate_json_output() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--format", "json", "SELECT 1"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["blocked"] is False
    assert "effective_sql" in data


def test_validate_write_blocked() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "INSERT INTO t (a) VALUES (1)"])
    assert result.exit_code == 1
    assert "write operation blocked" in result.output
