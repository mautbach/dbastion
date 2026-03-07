# dbastion

A bastion for your database. AST-based SQL validation, cost estimation, and execution control for AI agents.

```bash
uv tool install dbastion          # install CLI globally
dbastion query "SELECT 1" --db duckdb:   # run it
```

## What it does

dbastion parses every SQL statement into an AST before it reaches the database.

- Classifies statements: read, dml, ddl, admin
- Blocks dangerous patterns: DELETE without WHERE, cartesian joins, `WHERE 1=1`, multi-statement, admin commands
- Injects LIMIT on unbounded SELECTs
- Dry-runs queries for cost estimation (BigQuery bytes/$, Snowflake partitions, Postgres EXPLAIN)
- Returns a decision: `allow`, `ask`, or `deny`

## Query + Approve model

`dbastion query` handles all SQL. Reads execute directly. Writes and expensive queries return `decision: ask` — pipe through `dbastion approve` to execute:

```bash
dbastion query "SELECT ..." --db prod                         # reads just work
dbastion query "INSERT ..." --db prod | dbastion approve      # writes need approval
dbastion query "SELECT ..." --db prod | dbastion approve      # expensive reads too
```

In agent harnesses (Claude Code, Codex), allow `query` freely and set `approve` to prompt:

```bash
dbastion install claude-code    # or: codex
```

This configures permissions so `query` and `schema` run freely; `approve` always prompts the human with the full SQL visible for review.

## Examples

### Read (executes)

```bash
$ dbastion query "SELECT id, name FROM users LIMIT 10" --db duckdb:path=app.db
```

```json
{
  "decision": "allow",
  "classification": "read",
  "columns": ["id", "name"],
  "rows": [{"id": 1, "name": "alice"}],
  "row_count": 1
}
```

### Write (needs approval)

```bash
$ dbastion query "DELETE FROM users WHERE id = 5" --db prod
```

```json
{
  "decision": "ask",
  "classification": "dml",
  "tables": ["users"],
  "approval_hint": "pipe this result to `dbastion approve` to execute"
}
```

```bash
$ dbastion query "DELETE FROM users WHERE id = 5" --db prod | dbastion approve
```

```json
{
  "decision": "approved",
  "effective_sql": "DELETE FROM users WHERE id = 5",
  "row_count": 1
}
```

### Dangerous (blocked)

```bash
$ dbastion query "DELETE FROM users" --db duckdb:
```

```json
{
  "decision": "deny",
  "classification": "dml",
  "blocked": true,
  "diagnostics": [
    {
      "code": "Q0201",
      "message": "DELETE without WHERE clause",
      "notes": ["this would affect all rows in the table"]
    }
  ]
}
```

## Install

```bash
uv tool install 'dbastion[postgres]'    # PostgreSQL
uv tool install 'dbastion[bigquery]'    # BigQuery
uv tool install 'dbastion[duckdb]'      # DuckDB
uv tool install 'dbastion[snowflake]'   # Snowflake
uv tool install 'dbastion[clickhouse]'  # ClickHouse
uv tool install 'dbastion[all]'         # everything
```

Also available via pip:

```bash
pip install 'dbastion[all]'
```

Both `dbastion` and `dbast` (short alias) work after install.

## Supported databases

| Database   | Dry-run | Cost estimation |
|------------|---------|-----------------|
| PostgreSQL | EXPLAIN (JSON) | row estimates |
| BigQuery   | native dry-run | bytes scanned, USD |
| Snowflake  | EXPLAIN USING JSON | partitions, bytes |
| ClickHouse | EXPLAIN ESTIMATE | rows, bytes |
| DuckDB     | EXPLAIN | query plan |

## Safety checks

| Code | Check | Action |
|------|-------|--------|
| Q0201 | DELETE without WHERE | blocked |
| Q0203 | UPDATE without WHERE | blocked |
| Q0202 | Multiple statements | blocked |
| Q0204 | CROSS JOIN / cartesian product | warning |
| Q0205 | Constant WHERE condition (`1=1`) | warning |
| Q0303 | Admin commands (GRANT, COPY, etc.) | blocked |
| Q0601 | Unbounded SELECT | auto-LIMIT injected |

## Commands

```
dbastion query <sql>          Execute reads, validate writes (→ approve)
dbastion approve              Approve and execute a blocked query (stdin pipe)
dbastion validate <sql>       Validate without executing
dbastion schema ls [schema]   List schemas or tables
dbastion schema show <table>  Show table columns and metadata
dbastion connect              Manage named connections
dbastion auth                 Manage credentials
```

## Options

```
--db <conn>          Connection name or type:key=val
--format json|text   Output format (default: json)
--limit N            Auto-LIMIT value (default: 1000, 0 to disable)
--dry-run            Estimate cost only, don't execute
--from-stdin         Read SQL from stdin (query only)
```

Cost thresholds are configured per-connection in `~/.dbastion/connections.toml`:

```toml
[prod]
type = "bigquery"
project = "my-project"
max_gb = "50"
max_usd = "5"
```

## License

Apache-2.0
