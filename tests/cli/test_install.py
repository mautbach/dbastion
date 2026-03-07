"""Tests for dbastion install / uninstall commands."""

from __future__ import annotations

import json

from click.testing import CliRunner

from dbastion.cli import main


class TestClaudeCodeInstall:
    """Claude Code: permissions.allow + permissions.ask in settings.json."""

    def test_fresh_install(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["install", "claude-code"])
        assert result.exit_code == 0

        settings = json.loads((tmp_path / ".claude" / "settings.json").read_text())
        allow = settings["permissions"]["allow"]
        ask = settings["permissions"]["ask"]

        assert "Bash(dbastion query *)" in allow
        assert "Bash(dbast schema *)" in allow
        assert "Bash(dbastion approve *)" in ask
        assert "Bash(dbast approve *)" in ask
        # approve must NOT be in allow
        assert "Bash(dbastion approve *)" not in allow

    def test_preserves_existing_settings(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        settings_dir = tmp_path / ".claude"
        settings_dir.mkdir()
        (settings_dir / "settings.json").write_text(json.dumps({
            "permissions": {
                "allow": ["Bash(git:*)", "Bash(npm:*)"],
                "deny": ["Read(./.env)"],
            },
            "model": "claude-sonnet-4-6",
        }))

        runner = CliRunner()
        result = runner.invoke(main, ["install", "claude-code"])
        assert result.exit_code == 0

        settings = json.loads((settings_dir / "settings.json").read_text())
        # Existing settings preserved
        assert settings["model"] == "claude-sonnet-4-6"
        assert "Bash(git:*)" in settings["permissions"]["allow"]
        assert "Bash(npm:*)" in settings["permissions"]["allow"]
        assert "Read(./.env)" in settings["permissions"]["deny"]
        # dbastion added
        assert "Bash(dbastion query *)" in settings["permissions"]["allow"]
        assert "Bash(dbastion approve *)" in settings["permissions"]["ask"]

    def test_idempotent(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        runner.invoke(main, ["install", "claude-code"])
        first = (tmp_path / ".claude" / "settings.json").read_text()

        result = runner.invoke(main, ["install", "claude-code"])
        assert result.exit_code == 0
        assert "already configured" in result.output
        assert (tmp_path / ".claude" / "settings.json").read_text() == first

    def test_uninstall_removes_only_dbastion(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        settings_dir = tmp_path / ".claude"
        settings_dir.mkdir()
        (settings_dir / "settings.json").write_text(json.dumps({
            "permissions": {
                "allow": ["Bash(git:*)", "Bash(dbastion query *)"],
                "ask": ["Bash(dbastion approve *)"],
            },
        }))

        runner = CliRunner()
        result = runner.invoke(main, ["uninstall", "claude-code"])
        assert result.exit_code == 0

        settings = json.loads((settings_dir / "settings.json").read_text())
        assert "Bash(git:*)" in settings["permissions"]["allow"]
        assert "Bash(dbastion query *)" not in settings["permissions"]["allow"]
        assert "Bash(dbastion approve *)" not in settings["permissions"]["ask"]

    def test_uninstall_preserves_user_authored_dbastion_rules(self, tmp_path, monkeypatch) -> None:
        """User-authored rules like Bash(dbastion connect *) are not removed."""
        monkeypatch.chdir(tmp_path)
        settings_dir = tmp_path / ".claude"
        settings_dir.mkdir()
        (settings_dir / "settings.json").write_text(json.dumps({
            "permissions": {
                "allow": [
                    "Bash(dbastion query *)",
                    "Bash(dbastion connect *)",
                ],
                "ask": ["Bash(dbastion approve *)"],
            },
        }))

        runner = CliRunner()
        result = runner.invoke(main, ["uninstall", "claude-code"])
        assert result.exit_code == 0

        settings = json.loads((settings_dir / "settings.json").read_text())
        # Our rules removed
        assert "Bash(dbastion query *)" not in settings["permissions"]["allow"]
        assert "Bash(dbastion approve *)" not in settings["permissions"]["ask"]
        # User-authored rule preserved
        assert "Bash(dbastion connect *)" in settings["permissions"]["allow"]

    def test_approve_ask_rule_written_alongside_broad_allow(self, tmp_path, monkeypatch) -> None:
        """Approve rule is added to ask even when a broad Bash allow exists."""
        monkeypatch.chdir(tmp_path)
        settings_dir = tmp_path / ".claude"
        settings_dir.mkdir()
        (settings_dir / "settings.json").write_text(json.dumps({
            "permissions": {
                "allow": ["Bash"],
            },
        }))

        runner = CliRunner()
        runner.invoke(main, ["install", "claude-code"])

        settings = json.loads((settings_dir / "settings.json").read_text())
        # approve is in ask — Claude evaluates ask before allow
        assert "Bash(dbastion approve *)" in settings["permissions"]["ask"]
        # broad Bash allow is preserved, not removed
        assert "Bash" in settings["permissions"]["allow"]


class TestCodexInstall:
    """Codex: prefix_rule() in rules files."""

    def test_project_path(self, tmp_path, monkeypatch) -> None:
        """Project rules go to .codex/rules/."""
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["install", "codex"])
        assert result.exit_code == 0

        rules_path = tmp_path / ".codex" / "rules" / "dbastion.rules"
        assert rules_path.exists()
        content = rules_path.read_text()
        assert 'pattern = ["dbastion", "query"]' in content
        assert 'decision = "allow"' in content
        assert 'pattern = ["dbastion", "approve"]' in content
        assert 'decision = "prompt"' in content

    def test_idempotent(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        runner.invoke(main, ["install", "codex"])
        first = (tmp_path / ".codex" / "rules" / "dbastion.rules").read_text()

        result = runner.invoke(main, ["install", "codex"])
        assert result.exit_code == 0
        assert "already up to date" in result.output
        assert (tmp_path / ".codex" / "rules" / "dbastion.rules").read_text() == first

    def test_uninstall(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        runner.invoke(main, ["install", "codex"])
        rules_path = tmp_path / ".codex" / "rules" / "dbastion.rules"
        assert rules_path.exists()

        result = runner.invoke(main, ["uninstall", "codex"])
        assert result.exit_code == 0
        assert not rules_path.exists()

    def test_uninstall_not_found(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["uninstall", "codex"])
        assert result.exit_code == 0
        assert "not found" in result.output
