#!/usr/bin/env python3
"""Generate TPC-H data via DuckDB and load into PostgreSQL via COPY FROM STDIN.

Drops indexes and foreign keys before loading, recreates them after — this is
~3x faster than loading with constraints active at large scale factors.

Usage:
    python scripts/tpch_load.py                    # SF=0.01 (~10MB)
    python scripts/tpch_load.py --sf 1             # SF=1 (~1GB)
    python scripts/tpch_load.py --sf 50            # SF=50 (~50GB)

Environment variables:
    TPCH_SCALE_FACTOR   Scale factor override (default: 0.01)
    POSTGRES_DSN        Connection string (default: postgresql://dbastion:dbastion_test@localhost:5433/dbastion_test)
"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
import time

# FK-safe load order: parents before children.
TABLES = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]

CHUNK_SIZE = 1024 * 1024  # 1MB read chunks for streaming into Postgres.

# Recreated after bulk load — must match docker/postgres/init.sql.
FOREIGN_KEYS = [
    "ALTER TABLE tpch.nation ADD FOREIGN KEY (n_regionkey) REFERENCES tpch.region(r_regionkey)",
    "ALTER TABLE tpch.supplier ADD FOREIGN KEY (s_nationkey) REFERENCES tpch.nation(n_nationkey)",
    "ALTER TABLE tpch.partsupp ADD FOREIGN KEY (ps_partkey) REFERENCES tpch.part(p_partkey)",
    "ALTER TABLE tpch.partsupp ADD FOREIGN KEY (ps_suppkey) REFERENCES tpch.supplier(s_suppkey)",
    "ALTER TABLE tpch.customer ADD FOREIGN KEY (c_nationkey) REFERENCES tpch.nation(n_nationkey)",
    "ALTER TABLE tpch.orders ADD FOREIGN KEY (o_custkey) REFERENCES tpch.customer(c_custkey)",
    "ALTER TABLE tpch.lineitem ADD FOREIGN KEY (l_orderkey) REFERENCES tpch.orders(o_orderkey)",
    ("ALTER TABLE tpch.lineitem ADD FOREIGN KEY (l_partkey, l_suppkey)"
     " REFERENCES tpch.partsupp(ps_partkey, ps_suppkey)"),
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_nation_regionkey ON tpch.nation(n_regionkey)",
    "CREATE INDEX IF NOT EXISTS idx_supplier_nationkey ON tpch.supplier(s_nationkey)",
    "CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON tpch.customer(c_nationkey)",
    "CREATE INDEX IF NOT EXISTS idx_orders_custkey ON tpch.orders(o_custkey)",
    "CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON tpch.orders(o_orderdate)",
    "CREATE INDEX IF NOT EXISTS idx_lineitem_orderkey ON tpch.lineitem(l_orderkey)",
    "CREATE INDEX IF NOT EXISTS idx_lineitem_partkey ON tpch.lineitem(l_partkey)",
    "CREATE INDEX IF NOT EXISTS idx_lineitem_suppkey ON tpch.lineitem(l_suppkey)",
    "CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON tpch.lineitem(l_shipdate)",
    "CREATE INDEX IF NOT EXISTS idx_partsupp_suppkey ON tpch.partsupp(ps_suppkey)",
]


def generate_csv(sf: float, out_dir: str) -> dict[str, str]:
    """Generate TPC-H data via DuckDB, export each table to CSV."""
    import duckdb

    print(f"Generating TPC-H SF={sf} via DuckDB...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")

    paths = {}
    for table in TABLES:
        path = os.path.join(out_dir, f"{table}.csv")
        con.execute(f"COPY (SELECT * FROM {table}) TO '{path}' (FORMAT CSV, HEADER TRUE)")
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  {table}: {size_mb:.1f} MB")
        paths[table] = path

    con.close()
    print(f"  Generated in {time.time() - t0:.1f}s\n")
    return paths


def _drop_constraints(cur) -> None:
    """Drop all foreign keys and secondary indexes in tpch schema."""
    print("Dropping indexes and foreign keys...")
    t0 = time.time()

    # Drop all FKs (query pg_constraint for auto-generated names).
    cur.execute(
        "SELECT conname, relname "
        "FROM pg_constraint c JOIN pg_class r ON c.conrelid = r.oid "
        "WHERE c.connamespace = 'tpch'::regnamespace AND c.contype = 'f'"
    )
    fks = cur.fetchall()
    for conname, relname in fks:
        cur.execute(f"ALTER TABLE tpch.{relname} DROP CONSTRAINT IF EXISTS {conname}")

    # Drop all non-PK indexes.
    cur.execute(
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname = 'tpch' AND indexname NOT LIKE '%_pkey'"
    )
    idxs = cur.fetchall()
    for (idx,) in idxs:
        cur.execute(f"DROP INDEX IF EXISTS tpch.{idx}")

    print(f"  Done in {time.time() - t0:.1f}s\n")


def _recreate_constraints(cur) -> None:
    """Recreate foreign keys and indexes after bulk load."""
    print("Recreating indexes...")
    t0 = time.time()
    for stmt in INDEXES:
        cur.execute(stmt)
    print(f"  Indexes created in {time.time() - t0:.1f}s")

    print("Recreating foreign keys...")
    t1 = time.time()
    for stmt in FOREIGN_KEYS:
        cur.execute(stmt)
    print(f"  FKs created in {time.time() - t1:.1f}s\n")


def load_into_postgres(dsn: str, csv_paths: dict[str, str]) -> None:
    """Stream CSVs into PostgreSQL via COPY FROM STDIN."""
    import psycopg

    print("Loading into PostgreSQL...\n")
    t0 = time.time()

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET search_path TO tpch")

        # Phase 1: Drop constraints for fast bulk load.
        _drop_constraints(cur)

        # Phase 2: Truncate.
        for table in reversed(TABLES):
            cur.execute(f"TRUNCATE {table} CASCADE")

        # Phase 3: COPY data.
        print("Loading data...")
        for table in TABLES:
            t1 = time.time()
            csv_path = csv_paths[table]

            with (
                cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy,
                open(csv_path, "rb") as f,
            ):
                while data := f.read(CHUNK_SIZE):
                    copy.write(data)

            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table}: {count:,} rows ({time.time() - t1:.1f}s)")

        print()

        # Phase 4: Recreate constraints.
        _recreate_constraints(cur)

        # Phase 5: ANALYZE for accurate planner statistics.
        print("Running ANALYZE...")
        for table in TABLES:
            cur.execute(f"ANALYZE {table}")

    print(f"\nTotal load time: {time.time() - t0:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load TPC-H data into PostgreSQL")
    parser.add_argument(
        "--sf",
        type=float,
        default=float(os.environ.get("TPCH_SCALE_FACTOR", "0.01")),
        help="TPC-H scale factor (default: 0.01, ~10MB)",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "POSTGRES_DSN",
            "postgresql://dbastion:dbastion_test@localhost:5433/dbastion_test",
        ),
        help="PostgreSQL connection string",
    )
    args = parser.parse_args()

    print(f"TPC-H Loader — SF={args.sf}, DSN={args.dsn}\n")

    csv_dir = tempfile.mkdtemp(prefix="tpch_csv_")
    try:
        csv_paths = generate_csv(args.sf, csv_dir)
        load_into_postgres(args.dsn, csv_paths)
    finally:
        shutil.rmtree(csv_dir, ignore_errors=True)

    print("\nDone!")


if __name__ == "__main__":
    main()
