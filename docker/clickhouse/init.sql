-- TPC-H schema for ClickHouse.
-- Column names match DuckDB dbgen output (lowercase, prefixed).
-- No foreign keys (ClickHouse does not support them).

CREATE DATABASE IF NOT EXISTS tpch;

CREATE TABLE tpch.region (
    r_regionkey  UInt32,
    r_name       String,
    r_comment    Nullable(String)
) ENGINE = MergeTree() ORDER BY r_regionkey;

CREATE TABLE tpch.nation (
    n_nationkey  UInt32,
    n_name       String,
    n_regionkey  UInt32,
    n_comment    Nullable(String)
) ENGINE = MergeTree() ORDER BY n_nationkey;

CREATE TABLE tpch.part (
    p_partkey      UInt64,
    p_name         String,
    p_mfgr         String,
    p_brand        String,
    p_type         String,
    p_size         Int32,
    p_container    String,
    p_retailprice  Decimal(15, 2),
    p_comment      Nullable(String)
) ENGINE = MergeTree() ORDER BY p_partkey;

CREATE TABLE tpch.supplier (
    s_suppkey    UInt64,
    s_name       String,
    s_address    String,
    s_nationkey  UInt32,
    s_phone      String,
    s_acctbal    Decimal(15, 2),
    s_comment    Nullable(String)
) ENGINE = MergeTree() ORDER BY s_suppkey;

CREATE TABLE tpch.partsupp (
    ps_partkey     UInt64,
    ps_suppkey     UInt64,
    ps_availqty    UInt64,
    ps_supplycost  Decimal(15, 2),
    ps_comment     Nullable(String)
) ENGINE = MergeTree() ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE tpch.customer (
    c_custkey    UInt64,
    c_name       String,
    c_address    String,
    c_nationkey  UInt32,
    c_phone      String,
    c_acctbal    Decimal(15, 2),
    c_mktsegment String,
    c_comment    Nullable(String)
) ENGINE = MergeTree() ORDER BY c_custkey;

CREATE TABLE tpch.orders (
    o_orderkey      UInt64,
    o_custkey       UInt64,
    o_orderstatus   String,
    o_totalprice    Decimal(15, 2),
    o_orderdate     Date,
    o_orderpriority String,
    o_clerk         String,
    o_shippriority  Int32,
    o_comment       Nullable(String)
) ENGINE = MergeTree() ORDER BY o_orderkey;

CREATE TABLE tpch.lineitem (
    l_orderkey      UInt64,
    l_partkey       UInt64,
    l_suppkey       UInt64,
    l_linenumber    UInt64,
    l_quantity      Decimal(15, 2),
    l_extendedprice Decimal(15, 2),
    l_discount      Decimal(15, 2),
    l_tax           Decimal(15, 2),
    l_returnflag    String,
    l_linestatus    String,
    l_shipdate      Date,
    l_commitdate    Date,
    l_receiptdate   Date,
    l_shipinstruct  String,
    l_shipmode      String,
    l_comment       Nullable(String)
) ENGINE = MergeTree() ORDER BY (l_orderkey, l_linenumber);
