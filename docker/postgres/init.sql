-- TPC-H schema for PostgreSQL.
-- Column names match DuckDB dbgen output (lowercase, prefixed).

CREATE SCHEMA IF NOT EXISTS tpch;
SET search_path TO tpch;

CREATE TABLE region (
    r_regionkey  INTEGER       NOT NULL PRIMARY KEY,
    r_name       VARCHAR(25)   NOT NULL,
    r_comment    VARCHAR(152)
);

CREATE TABLE nation (
    n_nationkey  INTEGER       NOT NULL PRIMARY KEY,
    n_name       VARCHAR(25)   NOT NULL,
    n_regionkey  INTEGER       NOT NULL REFERENCES region(r_regionkey),
    n_comment    VARCHAR(152)
);

CREATE TABLE part (
    p_partkey      BIGINT        NOT NULL PRIMARY KEY,
    p_name         VARCHAR(55)   NOT NULL,
    p_mfgr         VARCHAR(25)   NOT NULL,
    p_brand        VARCHAR(10)   NOT NULL,
    p_type         VARCHAR(25)   NOT NULL,
    p_size         INTEGER       NOT NULL,
    p_container    VARCHAR(10)   NOT NULL,
    p_retailprice  DECIMAL(15,2) NOT NULL,
    p_comment      VARCHAR(23)
);

CREATE TABLE supplier (
    s_suppkey    BIGINT        NOT NULL PRIMARY KEY,
    s_name       VARCHAR(25)   NOT NULL,
    s_address    VARCHAR(40)   NOT NULL,
    s_nationkey  INTEGER       NOT NULL REFERENCES nation(n_nationkey),
    s_phone      VARCHAR(15)   NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101)
);

CREATE TABLE partsupp (
    ps_partkey     BIGINT        NOT NULL,
    ps_suppkey     BIGINT        NOT NULL,
    ps_availqty    BIGINT        NOT NULL,
    ps_supplycost  DECIMAL(15,2) NOT NULL,
    ps_comment     VARCHAR(199),
    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);

CREATE TABLE customer (
    c_custkey    BIGINT        NOT NULL PRIMARY KEY,
    c_name       VARCHAR(25)   NOT NULL,
    c_address    VARCHAR(40)   NOT NULL,
    c_nationkey  INTEGER       NOT NULL REFERENCES nation(n_nationkey),
    c_phone      VARCHAR(15)   NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment VARCHAR(10)   NOT NULL,
    c_comment    VARCHAR(117)
);

CREATE TABLE orders (
    o_orderkey      BIGINT        NOT NULL PRIMARY KEY,
    o_custkey       BIGINT        NOT NULL REFERENCES customer(c_custkey),
    o_orderstatus   VARCHAR(1)    NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE          NOT NULL,
    o_orderpriority VARCHAR(15)   NOT NULL,
    o_clerk         VARCHAR(15)   NOT NULL,
    o_shippriority  INTEGER       NOT NULL,
    o_comment       VARCHAR(79)
);

CREATE TABLE lineitem (
    l_orderkey      BIGINT        NOT NULL,
    l_partkey       BIGINT        NOT NULL,
    l_suppkey       BIGINT        NOT NULL,
    l_linenumber    BIGINT        NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    VARCHAR(1)    NOT NULL,
    l_linestatus    VARCHAR(1)    NOT NULL,
    l_shipdate      DATE          NOT NULL,
    l_commitdate    DATE          NOT NULL,
    l_receiptdate   DATE          NOT NULL,
    l_shipinstruct  VARCHAR(25)   NOT NULL,
    l_shipmode      VARCHAR(10)   NOT NULL,
    l_comment       VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey)
);

-- Indexes for realistic query planning.
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);
