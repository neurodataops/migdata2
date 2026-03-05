-- Object     : tpch_sf1.orders
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 7e45ae7d2d02c5cd649720486574e563a148a055b058939e3e01f379e30486e1
-- Generated  : 2026-03-05T17:04:00.673350
-- ---

CREATE TABLE tpch_sf1.orders (
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