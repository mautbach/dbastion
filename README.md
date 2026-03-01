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
- Dry-runs queries for cost estimation (BigQuery bytes/$, Postgres EXPLAIN)
- Returns a decision: `allow`, `ask`, or `deny`

## Two-command model

`dbastion query` executes reads, but only validates writes. `dbastion exec` executes writes.

In Claude Code, wire it like this:

```json
{
  "permissions": {
    "allow": ["Bash(dbastion query:*)"],
    "ask":   ["Bash(dbastion exec:*)"]
  }
}
```

The agent runs `query` freely. Writes require human approval through the harness, with the SQL visible in the command line.

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

### Write (validates only)

```bash
$ dbastion query "DELETE FROM users WHERE id = 5" --db postgres:dsn=postgresql://localhost/mydb
```

```json
{
  "decision": "ask",
  "classification": "dml",
  "tables": ["users"],
  "blocked": false
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
dbastion query <sql>          Reads execute, writes validate only
dbastion exec <sql>           Execute a validated write
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
--limit N            Auto-LIMIT value (default: 1000)
--no-limit           Disable auto-LIMIT
--dry-run            Estimate cost only, don't execute
--max-gb N           Block if scan exceeds N GB (default: 69, 0 to disable)
--max-usd N          Block if cost exceeds $N
--max-rows N         Block if rows exceed N
--from-stdin         Read SQL from stdin (query only)
```

## License

Apache-2.0
