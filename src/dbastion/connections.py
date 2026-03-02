"""Named connection management — ~/.dbastion/connections.toml."""

from __future__ import annotations

import os
import stat
import tomllib
from pathlib import Path

from dbastion.adapters._base import ConnectionConfig, DatabaseType

_CONNECTIONS_FILE = Path.home() / ".dbastion" / "connections.toml"


def _escape_toml_value(v: str) -> str:
    """Escape a string for safe inclusion in a TOML double-quoted value."""
    return v.replace("\\", "\\\\").replace('"', '\\"')


def _write_toml(data: dict[str, dict]) -> None:
    """Serialize connections dict to TOML and write with restricted permissions."""
    lines: list[str] = []
    for conn_name, entry in data.items():
        lines.append(f"[{conn_name}]")
        for k, v in entry.items():
            lines.append(f'{k} = "{_escape_toml_value(str(v))}"')
        lines.append("")

    _CONNECTIONS_FILE.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _CONNECTIONS_FILE.write_text("\n".join(lines))
    os.chmod(_CONNECTIONS_FILE, stat.S_IRUSR | stat.S_IWUSR)  # 0600


def _load_file() -> dict:
    if not _CONNECTIONS_FILE.exists():
        return {}
    return tomllib.loads(_CONNECTIONS_FILE.read_text())


def list_connections() -> dict[str, dict]:
    """Return all named connections as {name: {type, ...params}}."""
    return _load_file()


def get_connection(name: str) -> ConnectionConfig | None:
    """Look up a named connection. Returns None if not found."""
    data = _load_file()
    if name not in data:
        return None

    entry = data[name]
    db_type_str = entry.get("type")
    if db_type_str is None:
        raise ValueError(
            f"Connection '{name}' is missing required 'type' field"
        )

    try:
        db_type = DatabaseType(db_type_str)
    except ValueError as e:
        valid = ", ".join(t.value for t in DatabaseType)
        raise ValueError(
            f"Connection '{name}' has invalid type '{db_type_str}'. "
            f"Valid types: {valid}"
        ) from e

    # Separate cost thresholds from adapter params.
    threshold_keys = {"max_gb", "max_usd", "max_rows"}
    params = {k: str(v) for k, v in entry.items() if k != "type" and k not in threshold_keys}

    def _parse_threshold(key: str) -> float | None:
        val = entry.get(key)
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid value for '{key}' in connection '{name}': "
                f"{val!r} (expected a number)"
            ) from e

    return ConnectionConfig(
        name=name,
        db_type=db_type,
        params=params,
        max_gb=_parse_threshold("max_gb"),
        max_usd=_parse_threshold("max_usd"),
        max_rows=_parse_threshold("max_rows"),
    )


def save_connection(name: str, db_type: str, params: dict[str, str]) -> Path:
    """Save a named connection to the config file."""
    data = _load_file()
    data[name] = {"type": db_type, **params}
    _write_toml(data)
    return _CONNECTIONS_FILE


def remove_connection(name: str) -> bool:
    """Remove a named connection. Returns True if removed, False if not found."""
    data = _load_file()
    if name not in data:
        return False
    del data[name]
    if not data:
        _CONNECTIONS_FILE.unlink(missing_ok=True)
    else:
        _write_toml(data)
    return True
