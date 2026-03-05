-- Object     : tpch_sf10.orders
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 034618309084838709595f31f78fe861994213a3f49a8a861ee7ef2f014b5f0b
-- Generated  : 2026-03-05T17:04:00.689353
-- ---

CREATE TABLE tpch_sf10.orders (
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