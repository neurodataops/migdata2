-- Object     : tpch_sf1000.orders
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : d4a5d1a9f42cdd07b177f3cd49e6159acfa252902631a34aa371f9202a1bad87
-- Generated  : 2026-03-08T16:35:47.560843
-- ---

CREATE TABLE tpch_sf1000.orders (
 o_orderkey NUMBER NOT NULL,
 o_custkey NUMBER NOT NULL,
 o_orderstatus TEXT NOT NULL,
 o_totalprice NUMBER NOT NULL,
 o_orderdate DATE NOT NULL,
 o_orderpriority TEXT NOT NULL,
 o_clerk TEXT NOT NULL,
 o_shippriority NUMBER NOT NULL,
 o_comment TEXT NOT NULL
)

USING DELTA;